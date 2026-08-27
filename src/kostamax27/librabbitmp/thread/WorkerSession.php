<?php

declare(strict_types=1);

namespace kostamax27\librabbitmp\thread;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;

/**
 * @internal
 */
final class WorkerSession{
	/**
	 * @param array<string, array{queue: string, no_ack: bool, exclusive: bool, arguments: array<string, mixed>, topic: array{string, string}|null}> $consumers indexed by consumer tag; "topic" holds [exchange, pattern] for topic subscriptions
	 * @param array{int, int, bool}|null $qos
	 */
	public function __construct(
		public AMQPStreamConnection $connection,
		public AMQPChannel $channel,
		public int $generation = 1,
		public array $consumers = [],
		public ?array $qos = null
	){}
}
