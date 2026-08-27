<?php

declare(strict_types=1);

namespace kostamax27\librabbitmp;

use SOFe\AwaitGenerator\Await;

/**
 * A message delivered by the broker.
 */
final class Delivery{
	/**
	 * @internal
	 *
	 * @param string $exchange "" for the default exchange
	 * @param array<string, mixed> $headers AMQP basic properties under php-amqplib's names ("reply_to", "correlation_id", "delivery_mode", ...) merged flat with custom application headers
	 *
	 * @param string|null $consumer_tag null for {@see RabbitMQ::get()} results
	 */
	public function __construct(
		readonly private RabbitMQ $connection,
		readonly public ?string $consumer_tag,
		readonly public int $delivery_tag,
		readonly public bool $redelivered,
		readonly public string $exchange,
		readonly public string $routing_key,
		readonly public array $headers,
		readonly public string $body,
		readonly public int $generation
	){}

	public function getHeader(string $name, mixed $default = null) : mixed{
		return $this->headers[$name] ?? $default;
	}

	/**
	 * Acknowledges this message. If the connection was re-established since
	 * delivery, the delivery tag is stale and this resolves as a no-op (the
	 * broker redelivers unacked messages on the new connection anyway).
	 *
	 * @param bool $multiple also acknowledge all earlier unacknowledged deliveries
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, void>
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function ack(bool $multiple = false) : \Generator{
		yield from $this->connection->ack($this, $multiple);
	}

	/**
	 * Rejects this message. Same stale-tag semantics as {@see Delivery::ack()}.
	 *
	 * @param bool $requeue requeue instead of discarding (or dead-lettering)
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, void>
	 * @throws ConnectionException if the connection is lost, closed, or cannot be established
	 * @throws OperationException if the broker rejects the operation
	 */
	public function nack(bool $requeue = true, bool $multiple = false) : \Generator{
		yield from $this->connection->nack($this, $multiple, $requeue);
	}
}
