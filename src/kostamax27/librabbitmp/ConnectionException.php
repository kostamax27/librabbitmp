<?php

declare(strict_types=1);

namespace kostamax27\librabbitmp;

/**
 * Thrown when the connection to the broker could not be established, was
 * lost, or an operation was scheduled on a closed connection.
 */
final class ConnectionException extends RabbitMQException{
}
