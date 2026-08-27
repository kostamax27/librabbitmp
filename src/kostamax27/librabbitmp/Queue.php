<?php

declare(strict_types=1);

namespace kostamax27\librabbitmp;

use SOFe\AwaitGenerator\Await;
use SOFe\AwaitGenerator\Traverser;

/**
 * Point-to-point messaging over a named durable queue, obtained from
 * {@see RabbitMQ::queue()}. Messages are stored until exactly one consumer
 * processes and acknowledges them (work queue pattern).
 */
final class Queue{
	/**
	 * @internal use {@see RabbitMQ::queue()}
	 */
	public function __construct(
		readonly private RabbitMQ $connection,
		readonly public string $name
	){}

	/**
	 * Publishes directly to this queue via the default exchange. Pass
	 * {@see MessageProperties::persistent()} for messages that must survive
	 * a broker restart.
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, void>
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function publish(string $body, ?MessageProperties $properties = null) : \Generator{
		yield from $this->connection->publish($body, $properties, "", $this->name);
	}

	/**
	 * Starts consuming with manual acknowledgement: spawn a coroutine from
	 * $handler and `yield from $delivery->ack()` once the message has been
	 * processed. Unacknowledged messages are redelivered. Call
	 * {@see RabbitMQ::qos()} first to bound deliveries in flight.
	 *
	 * @param \Closure(Delivery) : void $handler runs on the main thread
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, Subscription>
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function consume(\Closure $handler) : \Generator{
		return yield from $this->connection->consume($this->name, $handler);
	}

	/**
	 * Like {@see Queue::consume()}, but returns the deliveries as an async
	 * stream instead of invoking a callback - messages are processed
	 * strictly one after another:
	 *
	 * <code>
	 * yield from $rabbit->qos(prefetch_count: 1);
	 * $messages = $queue->messages();
	 * while(yield from $messages->next($delivery)){
	 *     //...
	 *     yield from $delivery->ack();
	 * }
	 * </code>
	 *
	 * The loop ends when the consumer is lost; break out early with
	 * `yield from $messages->interrupt()`, which cancels the consumer and
	 * requeues deliveries that were buffered but never iterated.
	 *
	 * `Traverser::next()` rejects with {@see OperationException} /
	 * {@see ConnectionException} under the same conditions as the methods
	 * above.
	 *
	 * @return Traverser<Delivery>
	 */
	public function messages() : Traverser{
		return $this->connection->stream(fn(\Closure $handler) : \Generator => $this->connection->consume($this->name, $handler), nack_unconsumed: true);
	}

	/**
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, int> the number of messages purged
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function purge() : \Generator{
		return yield from $this->connection->queuePurge($this->name);
	}
}
