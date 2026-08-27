<?php

declare(strict_types=1);

namespace kostamax27\librabbitmp;

use SOFe\AwaitGenerator\Await;

/**
 * A running consumer, returned by {@see Topic::subscribe()},
 * {@see Queue::consume()} and {@see RabbitMQ::consume()}.
 */
final class Subscription{
	/**
	 * @internal
	 */
	public function __construct(
		readonly private RabbitMQ $connection,
		readonly public string $consumer_tag,
		readonly public string $queue
	){}

	/**
	 * False after {@see Subscription::cancel()} or after the consumer was
	 * lost ({@see RabbitMQ::onConsumerLost()}).
	 */
	public function isActive() : bool{
		return $this->connection->hasConsumer($this->consumer_tag);
	}

	/**
	 * Registers $handler to run once when this subscription stops receiving
	 * deliveries for any reason other than {@see Subscription::cancel()}: the
	 * consumer could not be restored after a recovery, the connection was
	 * lost permanently, or the connection was closed. Runs immediately if
	 * already inactive.
	 *
	 * @param \Closure() : void $handler
	 */
	public function onLost(\Closure $handler) : void{
		$this->connection->watchConsumer($this->consumer_tag, $handler);
	}

	/**
	 * Stops consuming; no-op if no longer active. Deliveries already in
	 * flight may still invoke the handler.
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, void>
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function cancel() : \Generator{
		if($this->isActive()){
			yield from $this->connection->cancel($this->consumer_tag);
		}
	}
}
