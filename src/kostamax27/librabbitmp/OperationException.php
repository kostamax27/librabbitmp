<?php

declare(strict_types=1);

namespace kostamax27\librabbitmp;

/**
 * Thrown when the broker rejects a specific operation (e.g. a passive declare
 * of a missing queue, or publishing to a nonexistent exchange). The
 * connection itself is unaffected.
 */
final class OperationException extends RabbitMQException{
	public function __construct(
		readonly public string $operation,
		string $error_message,
		int $error_code = 0,
		?\Throwable $previous = null
	){
		parent::__construct("RabbitMQ operation \"{$operation}\" failed: {$error_message}", $error_code, $previous);
	}
}
