<?php

declare(strict_types=1);

namespace kostamax27\librabbitmp;

/**
 * Result of {@see RabbitMQ::queueDeclare()}.
 */
final class QueueInfo{
	public function __construct(
		readonly public string $name,
		readonly public int $message_count,
		readonly public int $consumer_count
	){}
}
