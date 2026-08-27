<?php

declare(strict_types=1);

namespace kostamax27\librabbitmp;

use SOFe\AwaitGenerator\Await;
use SOFe\AwaitGenerator\Traverser;

/**
 * Publish/subscribe over a topic exchange, obtained from {@see RabbitMQ::topic()}.
 * Routing keys are dot-separated words ("chat.staff"); binding patterns may use
 * "*" (exactly one word) and "#" (zero or more words).
 */
final class Topic{
	/**
	 * @internal use {@see RabbitMQ::topic()}
	 */
	public function __construct(
		readonly private RabbitMQ $connection,
		readonly public string $exchange
	){}

	/**
	 * Delivers a copy of the message to every currently connected subscriber
	 * whose pattern matches $routing_key. Nothing is stored for subscribers
	 * that are offline - use {@see Queue} for that.
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, void>
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function publish(string $routing_key, string $body, ?MessageProperties $properties = null) : \Generator{
		yield from $this->connection->publish($body, $properties, $this->exchange, $routing_key);
	}

	/**
	 * Declares an exclusive broker-named queue for this subscriber, binds
	 * $pattern and starts consuming with auto-acknowledgement. $handler runs
	 * on the main thread; spawn a coroutine inside it if it needs to await.
	 *
	 * @param \Closure(Delivery) : void $handler
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, Subscription>
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function subscribe(string $pattern, \Closure $handler) : \Generator{
		return yield from $this->connection->subscribeTopic($this->exchange, $pattern, $handler);
	}

	/**
	 * Like {@see Topic::subscribe()}, but returns the deliveries as an async
	 * stream instead of invoking a callback:
	 *
	 * <code>
	 * $messages = $topic->messages("chat.*");
	 * while(yield from $messages->next($delivery)){
	 *     //...
	 * }
	 * </code>
	 *
	 * The loop ends when the subscription is lost; break out early with
	 * `yield from $messages->interrupt()`, which cancels the subscription.
	 *
	 * `Traverser::next()` rejects with {@see OperationException} /
	 * {@see ConnectionException} under the same conditions as the methods
	 * above.
	 *
	 * @return Traverser<Delivery>
	 */
	public function messages(string $pattern) : Traverser{
		return $this->connection->stream(fn(\Closure $handler) : \Generator => $this->connection->subscribeTopic($this->exchange, $pattern, $handler), nack_unconsumed: false);
	}
}
