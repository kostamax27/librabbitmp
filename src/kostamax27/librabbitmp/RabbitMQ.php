<?php

declare(strict_types=1);

namespace kostamax27\librabbitmp;

use kostamax27\librabbitmp\thread\CommandQueue;
use kostamax27\librabbitmp\thread\EventQueue;
use kostamax27\librabbitmp\thread\EventType;
use kostamax27\librabbitmp\thread\Opcode;
use kostamax27\librabbitmp\thread\RabbitMQThread;
use pocketmine\plugin\PluginBase;
use SOFe\AwaitGenerator\Await;
use SOFe\AwaitGenerator\Traverser;
use function array_shift;
use function count;
use function preg_replace;
use function strtolower;

/**
 * @see librabbitmp::create()
 */
final class RabbitMQ{
	/** @internal */
	readonly public RabbitMQThread $thread;

	readonly private CommandQueue $commands;
	readonly private EventQueue $events;
	readonly private int $sleeper_notifier_id;
	readonly private string $consumer_tag_prefix;

	private int $next_command_id = 0;
	private int $next_consumer_id = 0;
	private bool $closed = false;

	/** @var array<int, array{\Closure(mixed) : void, \Closure(RabbitMQException) : void, Opcode}> */
	private array $pending = [];

	/** @var array<string, \Closure(Delivery) : void> */
	private array $consumer_handlers = [];

	/** @var list<\Closure(ConnectionException, bool) : void> */
	private array $connection_lost_handlers = [];

	/** @var list<\Closure() : void> */
	private array $connection_restored_handlers = [];

	/** @var list<\Closure(string, string) : void> */
	private array $consumer_lost_handlers = [];

	/** @var array<string, list<\Closure() : void>> */
	private array $consumer_lost_watchers = [];

	/**
	 * @internal use {@see librabbitmp::create()}
	 */
	public function __construct(
		readonly private PluginBase $plugin,
		readonly public ConnectionSettings $settings,
		string $worker_autoloader
	){
		$this->commands = new CommandQueue();
		$this->events = new EventQueue();

		$sleeper_entry = $plugin->getServer()->getTickSleeper()->addNotifier($this->processEvents(...));
		$this->sleeper_notifier_id = $sleeper_entry->getNotifierId();

		$this->consumer_tag_prefix = "librabbitmp:" . strtolower(preg_replace("/[^A-Za-z0-9_\\-]/", "", $plugin->getName())) . ":";
		$this->thread = new RabbitMQThread($this->commands, $this->events, $sleeper_entry, $plugin->getServer()->getLogger(), $this->settings, $worker_autoloader);
	}

	/**
	 * Declares a durable topic exchange and returns a handle for
	 * publish/subscribe messaging over it.
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, Topic>
	 * @throws OperationException if the broker rejects the operation
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws \InvalidArgumentException if $exchange is empty
	 */
	public function topic(string $exchange) : \Generator{
		$exchange !== "" || throw new \InvalidArgumentException("Topic exchange name must not be empty");
		yield from $this->exchangeDeclare($exchange, ExchangeType::TOPIC, durable: true);
		return new Topic($this, $exchange);
	}

	/**
	 * Declares a durable named queue and returns a handle for work queue
	 * messaging over it.
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, Queue>
	 * @throws OperationException if the broker rejects the operation
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws \InvalidArgumentException if $name is empty
	 */
	public function queue(string $name) : \Generator{
		$name !== "" || throw new \InvalidArgumentException("Queue name must not be empty");
		yield from $this->queueDeclare($name, durable: true);
		return new Queue($this, $name);
	}

	/**
	 * Declares an exchange, creating it if it does not exist.
	 *
	 * @param string $exchange exchange name
	 * @param ExchangeType $type exchange routing type
	 * @param bool $passive if true, only checks that the exchange exists (with the same parameters)
	 * @param bool $durable whether the exchange survives broker restarts
	 * @param bool $auto_delete whether the exchange is deleted once all queues have finished using it
	 * @param bool $internal whether the exchange may only be published to via exchange-to-exchange bindings
	 * @param array<string, mixed> $arguments optional AMQP arguments (e.g. "alternate-exchange")
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, void>
	 * @throws OperationException if the broker rejects the operation
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 */
	public function exchangeDeclare(string $exchange, ExchangeType $type, bool $passive = false, bool $durable = false, bool $auto_delete = false, bool $internal = false, array $arguments = []) : \Generator{
		yield from $this->schedule(Opcode::EXCHANGE_DECLARE, [$exchange, $type->value, $passive, $durable, $auto_delete, $internal, $arguments]);
	}

	/**
	 * Deletes an exchange.
	 *
	 * @param string $exchange exchange name
	 * @param bool $if_unused refuse deletion if the exchange still has bindings
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, void>
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function exchangeDelete(string $exchange, bool $if_unused = false) : \Generator{
		yield from $this->schedule(Opcode::EXCHANGE_DELETE, [$exchange, $if_unused]);
	}

	/**
	 * Declares a queue, creating it if it does not exist. Pass an empty name
	 * to let the broker generate one (returned in {@see QueueInfo::$name}).
	 *
	 * @param string $queue queue name, or "" for a broker-generated name
	 * @param bool $passive if true, only checks that the queue exists (with the same parameters)
	 * @param bool $durable whether the queue survives broker restarts
	 * @param bool $exclusive whether the queue may only be used by this connection and is deleted when it closes
	 * @param bool $auto_delete whether the queue is deleted once the last consumer unsubscribes
	 * @param array<string, mixed> $arguments optional AMQP arguments (e.g. "x-message-ttl", "x-dead-letter-exchange")
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, QueueInfo>
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function queueDeclare(string $queue = "", bool $passive = false, bool $durable = false, bool $exclusive = false, bool $auto_delete = false, array $arguments = []) : \Generator{
		/** @var array{string, int, int} $result */
		$result = yield from $this->schedule(Opcode::QUEUE_DECLARE, [$queue, $passive, $durable, $exclusive, $auto_delete, $arguments]);
		return new QueueInfo(name: $result[0], message_count: $result[1], consumer_count: $result[2]);
	}

	/**
	 * Binds a queue to an exchange.
	 *
	 * @param string $routing_key binding key, may contain "*" / "#" wildcards on topic exchanges
	 * @param array<string, mixed> $arguments optional AMQP arguments (used by headers exchanges)
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, void>
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function queueBind(string $queue, string $exchange, string $routing_key = "", array $arguments = []) : \Generator{
		yield from $this->schedule(Opcode::QUEUE_BIND, [$queue, $exchange, $routing_key, $arguments]);
	}

	/**
	 * Removes a binding between a queue and an exchange.
	 *
	 * @param array<string, mixed> $arguments must match the arguments the binding was created with
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, void>
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function queueUnbind(string $queue, string $exchange, string $routing_key = "", array $arguments = []) : \Generator{
		yield from $this->schedule(Opcode::QUEUE_UNBIND, [$queue, $exchange, $routing_key, $arguments]);
	}

	/**
	 * Deletes a queue.
	 *
	 * @param bool $if_unused refuse deletion if the queue has consumers
	 * @param bool $if_empty refuse deletion if the queue has messages
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, int> the number of messages deleted alongside the queue
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function queueDelete(string $queue, bool $if_unused = false, bool $if_empty = false) : \Generator{
		return yield from $this->schedule(Opcode::QUEUE_DELETE, [$queue, $if_unused, $if_empty]);
	}

	/**
	 * Removes all messages from a queue that do not await acknowledgement.
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, int> the number of messages purged
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function queuePurge(string $queue) : \Generator{
		return yield from $this->schedule(Opcode::QUEUE_PURGE, [$queue]);
	}

	/**
	 * Publishes a message. The coroutine resolves once the worker thread has
	 * written the message to the connection - the broker does not confirm
	 * routing, so publishing to a nonexistent exchange rejects while an
	 * unroutable routing key resolves silently (standard AMQP behaviour).
	 *
	 * @param string $body raw message payload
	 * @param string $exchange exchange to publish to, "" for the default exchange
	 * @param string $routing_key routing key (queue name when using the default exchange)
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, void>
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function publish(string $body, ?MessageProperties $properties = null, string $exchange = "", string $routing_key = "") : \Generator{
		yield from $this->schedule(Opcode::PUBLISH, [$body, $properties?->toDriverHeaders() ?? [], $exchange, $routing_key]);
	}

	/**
	 * Starts consuming messages from a queue. $handler runs on the main
	 * thread; spawn a coroutine inside it to await further operations such
	 * as {@see Delivery::ack()}.
	 *
	 * @param \Closure(Delivery) : void $handler
	 * @param bool $no_ack if true, the broker considers messages acknowledged as soon as they are sent (fire-and-forget)
	 * @param bool $exclusive request exclusive consumer access to the queue
	 * @param array<string, mixed> $arguments optional AMQP arguments (e.g. "x-priority")
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, Subscription>
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function consume(string $queue, \Closure $handler, bool $no_ack = false, bool $exclusive = false, array $arguments = []) : \Generator{
		$consumer_tag = $this->consumer_tag_prefix . $this->next_consumer_id++;
		$this->consumer_handlers[$consumer_tag] = $handler;
		try{
			/** @var string $tag */
			$tag = yield from $this->schedule(Opcode::CONSUME, [$queue, $consumer_tag, $no_ack, $exclusive, $arguments]);
		}catch(ConnectionException $e){
			unset($this->consumer_handlers[$consumer_tag]);
			throw $e;
		}
		return new Subscription($this, $tag, $queue);
	}

	/**
	 * @internal use {@see Topic::subscribe()}
	 *
	 * @param \Closure(Delivery) : void $handler
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, Subscription>
	 * @throws OperationException if the broker rejects the operation
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 */
	public function subscribeTopic(string $exchange, string $pattern, \Closure $handler) : \Generator{
		$consumer_tag = $this->consumer_tag_prefix . $this->next_consumer_id++;
		$this->consumer_handlers[$consumer_tag] = $handler;
		try{
			/** @var array{string, string} $result */
			$result = yield from $this->schedule(Opcode::SUBSCRIBE_TOPIC, [$exchange, $pattern, $consumer_tag]);
		}catch(\Throwable $e){
			unset($this->consumer_handlers[$consumer_tag]);
			throw $e;
		}
		return new Subscription($this, $result[0], $result[1]);
	}

	/**
	 * @internal use {@see Subscription::isActive()}
	 */
	public function hasConsumer(string $consumer_tag) : bool{
		return isset($this->consumer_handlers[$consumer_tag]);
	}

	/**
	 * Cancels a consumer. Prefer {@see Subscription::cancel()}. Deliveries
	 * already in flight may still invoke the handler until the cancellation
	 * completes.
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, void>
	 * @throws OperationException if the broker rejects the operation
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws \InvalidArgumentException if $consumer_tag is unknown
	 */
	public function cancel(string $consumer_tag) : \Generator{
		isset($this->consumer_handlers[$consumer_tag]) || throw new \InvalidArgumentException("Unknown consumer tag \"{$consumer_tag}\"");
		yield from $this->schedule(Opcode::CANCEL, [$consumer_tag]);
		unset($this->consumer_handlers[$consumer_tag], $this->consumer_lost_watchers[$consumer_tag]);
	}

	/**
	 * Synchronously polls a single message from a queue ("pull" API). Prefer
	 * {@see RabbitMQ::consume()} for continuous consumption.
	 *
	 * @param bool $no_ack if true, the message does not need to be acknowledged
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, Delivery|null> the message, or null if the queue was empty
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function get(string $queue, bool $no_ack = false) : \Generator{
		/** @var array{string|null, int, bool, string, string, array<string, mixed>, string, int}|null $result */
		$result = yield from $this->schedule(Opcode::GET, [$queue, $no_ack]);
		return $result !== null ? $this->decodeDelivery($result) : null;
	}

	/**
	 * Acknowledges a delivered message. Prefer {@see Delivery::ack()}.
	 *
	 * @param bool $multiple also acknowledge all earlier unacknowledged deliveries on this channel
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, void>
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function ack(Delivery $delivery, bool $multiple = false) : \Generator{
		yield from $this->schedule(Opcode::ACK, [$delivery->delivery_tag, $delivery->generation, $multiple]);
	}

	/**
	 * Negatively acknowledges a delivered message. Prefer {@see Delivery::nack()}.
	 *
	 * @param bool $multiple also reject all earlier unacknowledged deliveries on this channel
	 * @param bool $requeue whether the broker should requeue the message instead of discarding (or dead-lettering) it
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, void>
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function nack(Delivery $delivery, bool $multiple = false, bool $requeue = true) : \Generator{
		yield from $this->schedule(Opcode::NACK, [$delivery->delivery_tag, $delivery->generation, $multiple, $requeue]);
	}

	/**
	 * Configures quality-of-service for this connection's channel, most
	 * commonly to bound how many unacknowledged deliveries the broker pushes
	 * at once.
	 *
	 * @param int $prefetch_size maximum unacknowledged message payload bytes, 0 for unlimited
	 * @param int $prefetch_count maximum unacknowledged deliveries, 0 for unlimited
	 * @param bool $global apply per-channel rather than per-consumer
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, void>
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function qos(int $prefetch_size = 0, int $prefetch_count = 0, bool $global = false) : \Generator{
		yield from $this->schedule(Opcode::QOS, [$prefetch_size, $prefetch_count, $global]);
	}

	/**
	 * $permanent is true when the worker will not retry (reconnection
	 * disabled, or {@see ConnectionSettings::$max_reconnect_attempts}
	 * exhausted).
	 *
	 * @param \Closure(ConnectionException, bool $permanent) : void $handler
	 */
	public function onConnectionLost(\Closure $handler) : void{
		$this->connection_lost_handlers[] = $handler;
	}

	/**
	 * Consumers, topic subscriptions and QoS settings have already been
	 * restored by the time the handler runs.
	 *
	 * @param \Closure() : void $handler
	 */
	public function onConnectionRestored(\Closure $handler) : void{
		$this->connection_restored_handlers[] = $handler;
	}

	/**
	 * Fires when a consumer could not be restored after a recovery -
	 * typically because its named queue no longer exists. Re-declare the
	 * queue and consume again to resume.
	 *
	 * @param \Closure(string $consumer_tag, string $reason) : void $handler
	 */
	public function onConsumerLost(\Closure $handler) : void{
		$this->consumer_lost_handlers[] = $handler;
	}

	/**
	 * Whether {@see RabbitMQ::close()} has been called or the connection was
	 * lost permanently. Operations on a closed connection reject with
	 * {@see ConnectionException}.
	 */
	public function isClosed() : bool{
		return $this->closed;
	}

	/**
	 * Blocks until all currently pending operations have been responded to.
	 * Useful before {@see RabbitMQ::close()} in onDisable().
	 */
	public function waitAll() : void{
		while(count($this->pending) > 0){
			if($this->thread->isTerminated()){
				$this->rejectAllPending(new ConnectionException("The RabbitMQ worker thread terminated unexpectedly"));
				break;
			}
			$this->events->waitForEvents(100_000);
			$this->processEvents();
		}
	}

	/**
	 * Closes the connection and joins the worker thread. Idempotent; all
	 * pending operations are rejected with {@see ConnectionException}.
	 */
	public function close() : void{
		if($this->closed){
			return;
		}
		$this->closed = true;
		$this->commands->invalidate();
		$this->thread->quit();
		$this->processEvents();
		$this->rejectAllPending(new ConnectionException("The RabbitMQ connection has been closed"));
		$this->dropAllConsumers();
		$this->plugin->getServer()->getTickSleeper()->removeNotifier($this->sleeper_notifier_id);
	}

	/**
	 * Registers $handler to run once when the consumer stops receiving
	 * deliveries for any reason other than {@see RabbitMQ::cancel()}: it
	 * could not be restored after a recovery, the connection was lost
	 * permanently, or the connection was closed. Runs immediately if the
	 * consumer is already gone.
	 *
	 * @internal use {@see Subscription::onLost()}
	 *
	 * @param \Closure() : void $handler
	 */
	public function watchConsumer(string $consumer_tag, \Closure $handler) : void{
		if(!isset($this->consumer_handlers[$consumer_tag])){
			$handler();
			return;
		}
		$this->consumer_lost_watchers[$consumer_tag][] = $handler;
	}

	/**
	 * Adapts a callback consumer into a {@see Traverser} of deliveries. The
	 * stream ends when the consumer is lost; interrupting it cancels the
	 * consumer and, if $nack_unconsumed, requeues deliveries that were
	 * buffered but never iterated.
	 *
	 * @internal use {@see Topic::messages()} / {@see Queue::messages()}
	 *
	 * @param \Closure(\Closure(Delivery) : void) : \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, Subscription> $subscribe
	 *
	 * @return Traverser<Delivery>
	 */
	public function stream(\Closure $subscribe, bool $nack_unconsumed) : Traverser{
		return Traverser::fromClosure(static function() use ($subscribe, $nack_unconsumed) : \Generator{
			/** @var list<Delivery|null> $buffer */
			$buffer = [];
			/** @var \Closure() : void|null $wakeup */
			$wakeup = null;
			$push = static function(?Delivery $delivery) use (&$buffer, &$wakeup) : void{
				$buffer[] = $delivery;
				if($wakeup !== null){
					$notify = $wakeup;
					$wakeup = null;
					$notify();
				}
			};
			/** @var Subscription $subscription */
			$subscription = yield from $subscribe($push);
			$subscription->onLost(static fn() => $push(null));
			try{
				while(true){
					if(count($buffer) === 0){
						//deliveries may arrive between iterations, while this
						//generator is suspended without an active next() call -
						//they land in $buffer with no $wakeup registered yet, so
						//the closure below must re-check before suspending
						yield from Await::promise(static function(\Closure $resolve) use (&$buffer, &$wakeup) : void{
							if(count($buffer) !== 0){
								$resolve(null);
							}else{
								$wakeup = $resolve;
							}
						});
					}
					$delivery = array_shift($buffer);
					if($delivery === null){
						break;
					}
					yield $delivery => Traverser::VALUE;
				}
			}finally{
				yield from $subscription->cancel();
				while($nack_unconsumed && ($delivery = array_shift($buffer)) !== null){
					try{
						yield from $delivery->nack();
					}catch(RabbitMQException){
						break;
					}
				}
			}
		});
	}

	private function dropAllConsumers() : void{
		$this->consumer_handlers = [];
		$watchers = $this->consumer_lost_watchers;
		$this->consumer_lost_watchers = [];
		foreach($watchers as $consumer_watchers){
			foreach($consumer_watchers as $watcher){
				$watcher();
			}
		}
	}

	/**
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, mixed>
	 * @throws ConnectionException
	 * @throws OperationException
	 */
	private function schedule(Opcode $opcode, array $params) : \Generator{
		return yield from Await::promise(function(\Closure $resolve, \Closure $reject) use ($opcode, $params) : void{
			!$this->closed || throw new ConnectionException("The RabbitMQ connection has been closed");
			$command_id = $this->next_command_id++;
			$this->pending[$command_id] = [$resolve, $reject, $opcode];
			try{
				$this->commands->schedule($command_id, $opcode, $params);
			}catch(ConnectionException $e){
				unset($this->pending[$command_id]);
				throw $e;
			}
		});
	}

	private function processEvents() : void{
		foreach($this->events->fetchAll() as [$type, $payload]){
			switch($type){
				case EventType::RESPONSE_OK:
					/** @var array{int, mixed} $payload */
					[$command_id, $result] = $payload;
					$handler = $this->pending[$command_id] ?? null;
					if($handler !== null){
						unset($this->pending[$command_id]);
						$handler[0]($result);
					}
					break;
				case EventType::RESPONSE_ERROR:
					/** @var array{int, string, int, bool} $payload */
					[$command_id, $message, $code, $is_connection_error] = $payload;
					$handler = $this->pending[$command_id] ?? null;
					if($handler !== null){
						unset($this->pending[$command_id]);
						$error = $is_connection_error ?
							new ConnectionException($message, $code) :
							new OperationException($handler[2]->operationName(), $message, $code);
						$handler[1]($error);
					}
					break;
				case EventType::DELIVERY:
					/** @var array{string|null, int, bool, string, string, array<string, mixed>, string, int} $payload */
					$this->dispatchDelivery($this->decodeDelivery($payload));
					break;
				case EventType::CONNECTION_LOST:
					/** @var array{string, bool} $payload */
					[$reason, $permanent] = $payload;
					$this->handleConnectionLost(new ConnectionException("Lost connection to RabbitMQ broker: {$reason}"), $permanent);
					break;
				case EventType::CONNECTION_RESTORED:
					foreach($this->connection_restored_handlers as $handler){
						$handler();
					}
					break;
				case EventType::CONSUMER_LOST:
					/** @var array{string, string} $payload */
					[$consumer_tag, $reason] = $payload;
					unset($this->consumer_handlers[$consumer_tag]);
					$this->plugin->getLogger()->warning("RabbitMQ consumer \"{$consumer_tag}\" was lost and will not receive further deliveries: {$reason}");
					$watchers = $this->consumer_lost_watchers[$consumer_tag] ?? [];
					unset($this->consumer_lost_watchers[$consumer_tag]);
					foreach($watchers as $watcher){
						$watcher();
					}
					foreach($this->consumer_lost_handlers as $handler){
						$handler($consumer_tag, $reason);
					}
					break;
			}
		}
	}

	/**
	 * @param array{string|null, int, bool, string, string, array<string, mixed>, string, int} $payload
	 */
	private function decodeDelivery(array $payload) : Delivery{
		return new Delivery(
			connection: $this,
			consumer_tag: $payload[0],
			delivery_tag: $payload[1],
			redelivered: $payload[2],
			exchange: $payload[3],
			routing_key: $payload[4],
			headers: $payload[5],
			body: $payload[6],
			generation: $payload[7]
		);
	}

	private function dispatchDelivery(Delivery $delivery) : void{
		$handler = $delivery->consumer_tag !== null ? ($this->consumer_handlers[$delivery->consumer_tag] ?? null) : null;
		if($handler === null){
			$this->plugin->getLogger()->debug("Dropping RabbitMQ delivery for unknown consumer \"" . ($delivery->consumer_tag ?? "?") . "\"");
			return;
		}
		try{
			$handler($delivery);
		}catch(\Throwable $e){
			//a throwing handler must not kill event processing for other
			//consumers - log and move on.
			$this->plugin->getLogger()->logException($e);
		}
	}

	private function handleConnectionLost(ConnectionException $error, bool $permanent) : void{
		$this->rejectAllPending($error);
		if($permanent && !$this->closed){
			$this->closed = true;
			$this->commands->invalidate();
			$this->dropAllConsumers();
			$this->plugin->getServer()->getTickSleeper()->removeNotifier($this->sleeper_notifier_id);
		}
		foreach($this->connection_lost_handlers as $handler){
			$handler($error, $permanent);
		}
	}

	private function rejectAllPending(ConnectionException $error) : void{
		$pending = $this->pending;
		$this->pending = [];
		foreach($pending as $handler){
			$handler[1]($error);
		}
	}
}
