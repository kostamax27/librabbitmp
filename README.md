# librabbitmp

Asynchronous RabbitMQ AMQP client for PocketMine-MP.

All network I/O runs on a dedicated worker thread, so the main thread never blocks on the broker.
The public API is built on [await-generator](https://github.com/SOF3/await-generator): every operation is a
coroutine you `yield from`, and every handler runs back on the main thread, where it is safe to touch the
server, players and other plugins.

```php
$topic = yield from $rabbit->topic("network");
yield from $topic->subscribe("player.*", fn(Delivery $d) => $this->getLogger()->info($d->body));
yield from $topic->publish("player.join", $payload);
```

## Features

- **Non-blocking.** Commands are queued to a worker thread; results come back through PocketMine's `SleeperHandler`, so there is no polling and no main-thread stall.
- **Two high-level patterns.** `Topic` for publish/subscribe (fan-out to every online server), `Queue` for durable work queues (each message handled by exactly one consumer).
- **Full AMQP surface underneath.** Exchange and queue declaration, bindings, routing, manual ack/nack, QoS/prefetch, pull-based `get()`, RPC via `reply_to` / `correlation_id`.
- **Automatic recovery.** On connection loss the worker reconnects and re-declares consumers, topic subscriptions and QoS settings by itself. Deliveries that arrived before the drop carry a generation marker, so stale acks are discarded instead of acknowledging the wrong message.
- **Callbacks or streams.** Consume with a `Closure`, or iterate deliveries as an await-generator `Traverser`.

## Quick start

`config.yml`:

```yml
rabbitmq:
  host: 127.0.0.1
  port: 5672
  vhost: /
  username: guest
  password: guest
```

`Loader.php`:

```php
final class Loader extends PluginBase{
    private RabbitMQ $rabbit;

    protected function onEnable() : void{
        $this->saveDefaultConfig();
        $this->rabbit = librabbitmp::create($this, ConnectionSettings::fromArray($this->getConfig()->get("rabbitmq", [])));

        Await::f2c(function() : \Generator{
            try{
                $topic = yield from $this->rabbit->topic("network");
                yield from $topic->subscribe("player.*", function(Delivery $delivery) : void{
                    $this->getServer()->broadcastMessage($delivery->body);
                });
                yield from $topic->publish("player.join", "Steve joined the lobby");
            }catch(RabbitMQException $e){
                $this->getLogger()->logException($e);
            }
        });
    }

    protected function onDisable() : void{
        if(isset($this->rabbit)){
            $this->rabbit->waitAll();
            $this->rabbit->close();
        }
    }
}
```

`librabbitmp::create()` blocks the main thread until the **first** connection attempt finishes (at most
`connection_timeout` seconds), so bad credentials or a wrong host fail loudly in `onEnable()` instead of
surfacing later in an unrelated operation. It throws `ConnectionException` on failure.

Always `close()` the connection in `onDisable()`; `waitAll()` beforehand lets operations that are already in
flight finish first.

## Connection settings

`ConnectionSettings::fromArray()` accepts the keys below; anything else throws `InvalidArgumentException`.

| Key                      | Default     | Description                                                        |
|--------------------------|-------------|--------------------------------------------------------------------|
| `host`                   | `127.0.0.1` | Broker host                                                        |
| `port`                   | `5672`      | Broker port                                                        |
| `vhost`                  | `/`         | Virtual host                                                       |
| `username`               | `guest`     | Login                                                              |
| `password`               | `guest`     | Password                                                           |
| `heartbeat`              | `30.0`      | Seconds between AMQP heartbeat frames                              |
| `connection-timeout`     | `10.0`      | Seconds to wait for a connection attempt                           |
| `poll-interval`          | `0.025`     | Seconds the worker lets the socket idle between command checks     |
| `reconnect`              | `true`      | Reconnect automatically after a connection loss                    |
| `reconnect-interval`     | `5.0`       | Seconds between reconnection attempts                              |
| `max-reconnect-attempts` | `0`         | `0` = retry forever                                                |
| `ssl`                    | *(none)*    | PHP SSL context options; presence switches the connection to AMQPS |

The constructor takes the same values as named arguments if you would rather not go through config.

## Topics - publish/subscribe

A `Topic` wraps a durable topic exchange. Routing keys are dot-separated words; binding patterns may use `*`
(exactly one word) and `#` (zero or more words). Every matching subscriber that is **online** gets a copy;
nothing is stored for servers that are down.

```php
$topic = yield from $rabbit->topic("network");

yield from $topic->publish("chat.staff", $json);

$subscription = yield from $topic->subscribe("chat.#", function(Delivery $delivery) : void{
    //runs on the main thread
});
```

`subscribe()` declares an exclusive, broker-named queue for this server, binds the pattern to it and consumes
with auto-acknowledgement.

## Queues - work queues

A `Queue` wraps a durable named queue. Messages wait in the broker until a consumer processes **and
acknowledges** them, so exactly one server handles each message and nothing is lost while consumers are
offline.

```php
$queue = yield from $rabbit->queue("reports");

yield from $queue->publish($json, MessageProperties::json()->persistent());

yield from $rabbit->qos(prefetch_count: 1);
yield from $queue->consume(function(Delivery $delivery) : void{
    Await::f2c(function() use ($delivery) : \Generator{
        //handle the message ...
        yield from $delivery->ack();
    });
});
```

Handlers are plain closures, so awaiting inside one means spawning a coroutine with `Await::f2c()`. Messages
that are never acked are redelivered; use `$delivery->nack(requeue: false)` to discard (or dead-letter) a
message you cannot handle. Call `qos()` before consuming to bound how many deliveries the broker pushes at
once.

## Streams instead of callbacks

`Queue::messages()` and `Topic::messages()` return a `Traverser<Delivery>`, which processes messages strictly
one at a time and keeps the flow readable:

```php
yield from $rabbit->qos(prefetch_count: 1);

$reports = $queue->messages();
while(yield from $reports->next($delivery)){
    //handle the message ...
    yield from $delivery->ack();
}
```

The loop ends when the consumer is lost or the connection closes. Break out early with
`yield from $reports->interrupt()`, which cancels the consumer and requeues deliveries that were buffered but
never iterated.

## Message properties

```php
$properties = MessageProperties::json()      //content-type: application/json
    ->persistent()                           //survives a broker restart in a durable queue
    ->expiresAfterMillis(60_000)             //per-message TTL
    ->correlationId($id)                     //RPC correlation
    ->replyTo("amq.rabbitmq.reply-to")       //RPC reply address
    ->header("server", $this->server_name);  //custom application header

yield from $queue->publish($body, $properties);
```

Only properties you set are sent; everything else keeps the AMQP defaults. On the receiving side,
`$delivery->getHeader("server")` reads them back.

## Low-level API

`RabbitMQ` exposes the AMQP methods directly when the `Topic` / `Queue` helpers are not enough:

| Method                                              | Purpose                                                                                  |
|-----------------------------------------------------|------------------------------------------------------------------------------------------|
| `exchangeDeclare()` / `exchangeDelete()`            | Declare or remove an exchange (`ExchangeType::DIRECT`, `FANOUT`, `TOPIC`, `HEADERS`)     |
| `queueDeclare()` / `queueDelete()` / `queuePurge()` | Manage queues; returns `QueueInfo` with name, message and consumer counts                |
| `queueBind()` / `queueUnbind()`                     | Manage bindings between queues and exchanges                                             |
| `publish()`                                         | Publish to an arbitrary exchange and routing key                                         |
| `consume()` / `cancel()`                            | Start or stop a consumer with full control over `no_ack`, `exclusive` and AMQP arguments |
| `get()`                                             | Pull a single message, or `null` if the queue is empty                                   |
| `ack()` / `nack()`                                  | Acknowledge or reject a delivery                                                         |
| `qos()`                                             | Set prefetch size/count                                                                  |

All of them accept AMQP `arguments` arrays, so queue features like `x-message-ttl`, `x-max-priority` and
`x-dead-letter-exchange` are available.

## Errors

Every failure is a `RabbitMQException`:

- **`OperationException`** - the broker rejected one operation (a passive declare of a missing queue, publishing to a nonexistent exchange, …). The connection itself is fine; `$e->operation` names the failed operation.
- **`ConnectionException`** - the connection could not be established, was lost, or the operation was scheduled on a closed connection.

Argument validation (empty queue names, an out-of-range priority, an unknown config key) throws
`InvalidArgumentException` immediately, before anything reaches the broker.

## Connection lifecycle

```php
$rabbit->onConnectionLost(function(ConnectionException $error, bool $permanent) : void{
    $this->getLogger()->warning("RabbitMQ unavailable: {$error->getMessage()}");
});

$rabbit->onConnectionRestored(function() : void{
    //consumers, subscriptions and QoS are already restored here
    $this->getLogger()->notice("RabbitMQ reconnected");
});

$rabbit->onConsumerLost(function(string $consumer_tag, string $reason) : void{
    //typically: the consumer's queue no longer exists - re-declare and consume again
});
```

`$permanent` is `true` when the worker will not retry, i.e. `reconnect` is disabled or
`max-reconnect-attempts` has been exhausted. A permanently lost connection is closed: further operations
reject with `ConnectionException` and `isClosed()` returns `true`.

Per-subscription, `Subscription::onLost()` fires once when that consumer stops receiving deliveries for any
reason other than an explicit `cancel()`, and `Subscription::isActive()` reports its current state.

## Examples

Four complete, runnable plugins live in [`examples/`](examples):

| Plugin                                        | Pattern | What it shows                                                                      |
|-----------------------------------------------|---------|------------------------------------------------------------------------------------|
| [JoinQuitMessages](examples/JoinQuitMessages) | Topic   | Network-wide join/quit broadcasts, delivery streams, connection lifecycle handlers |
| [StaffChat](examples/StaffChat)               | Topic   | Cross-server staff chat with a callback subscriber                                 |
| [WhoIs](examples/WhoIs)                       | RPC     | Request/response over direct reply-to with `correlation_id` and timeouts           |
