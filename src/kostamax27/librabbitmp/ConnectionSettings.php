<?php

declare(strict_types=1);

namespace kostamax27\librabbitmp;

use function gettype;
use function in_array;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;

/**
 * @see librabbitmp::create()
 */
final class ConnectionSettings{
	/**
	 * Reads settings from a config-friendly structure, e.g. the "rabbitmq"
	 * section of a plugin's config.yml:
	 *
	 * <code>
	 * rabbitmq:
	 *   host: 127.0.0.1
	 *   port: 5672
	 *   vhost: /
	 *   username: guest
	 *   password: guest
	 * </code>
	 *
	 * Unknown keys and wrongly-typed values throw instead of being silently
	 * ignored - a typo in config.yml should be a startup error, not a mystery.
	 *
	 * @throws \InvalidArgumentException
	 */
	public static function fromArray(array $data) : self{
		static $known = ["host", "port", "vhost", "username", "password", "heartbeat", "connection-timeout", "poll-interval", "reconnect", "reconnect-interval", "max-reconnect-attempts", "ssl"];
		foreach($data as $key => $_){
			in_array($key, $known, true) || throw new \InvalidArgumentException("Unknown RabbitMQ connection setting \"{$key}\"");
		}
		$ssl = $data["ssl"] ?? null;
		$ssl === null || is_array($ssl) || throw new \InvalidArgumentException("RabbitMQ connection setting \"ssl\" must be an array, got " . gettype($ssl));
		/** @var array<string, mixed>|null $ssl */
		$port = self::readInt($data, "port", 5672);
		($port > 0 && $port <= 65535) || throw new \InvalidArgumentException("RabbitMQ connection setting \"port\" must be a valid port number");
		return new self(
			host: self::readString($data, "host", "127.0.0.1"),
			port: $port,
			vhost: self::readString($data, "vhost", "/"),
			username: self::readString($data, "username", "guest"),
			password: self::readString($data, "password", "guest", allow_empty: true),
			heartbeat: self::readFloat($data, "heartbeat", 30.0, min: 1.0),
			connection_timeout: self::readFloat($data, "connection-timeout", 10.0, min: 0.1),
			poll_interval: self::readFloat($data, "poll-interval", 0.025, min: 0.001),
			reconnect: self::readBool($data, "reconnect", true),
			reconnect_interval: self::readFloat($data, "reconnect-interval", 5.0, min: 0.1),
			max_reconnect_attempts: self::readInt($data, "max-reconnect-attempts", 0, min: 0),
			ssl: $ssl
		);
	}

	private static function readString(array $data, string $key, string $default, bool $allow_empty = false) : string{
		$value = $data[$key] ?? $default;
		is_string($value) || throw new \InvalidArgumentException("RabbitMQ connection setting \"{$key}\" must be a string, got " . gettype($value));
		($allow_empty || $value !== "") || throw new \InvalidArgumentException("RabbitMQ connection setting \"{$key}\" must not be empty");
		return $value;
	}

	private static function readInt(array $data, string $key, int $default, ?int $min = null) : int{
		$value = $data[$key] ?? $default;
		is_int($value) || throw new \InvalidArgumentException("RabbitMQ connection setting \"{$key}\" must be an integer, got " . gettype($value));
		($min === null || $value >= $min) || throw new \InvalidArgumentException("RabbitMQ connection setting \"{$key}\" must be >= {$min}");
		return $value;
	}

	private static function readBool(array $data, string $key, bool $default) : bool{
		$value = $data[$key] ?? $default;
		is_bool($value) || throw new \InvalidArgumentException("RabbitMQ connection setting \"{$key}\" must be a boolean, got " . gettype($value));
		return $value;
	}

	private static function readFloat(array $data, string $key, float $default, float $min) : float{
		$value = $data[$key] ?? $default;
		(is_float($value) || is_int($value)) || throw new \InvalidArgumentException("RabbitMQ connection setting \"{$key}\" must be a number, got " . gettype($value));
		$value = (float) $value;
		$value >= $min || throw new \InvalidArgumentException("RabbitMQ connection setting \"{$key}\" must be >= {$min}");
		return $value;
	}

	/**
	 * @param float $heartbeat seconds between AMQP heartbeat frames (php-amqplib only supports whole seconds, so the fractional part is dropped)
	 * @param float $connection_timeout seconds to wait for a connection attempt before giving up
	 * @param float $poll_interval seconds the worker lets the socket idle between command checks
	 * @param bool $reconnect whether to automatically reconnect after losing the connection
	 * @param float $reconnect_interval seconds between reconnection attempts
	 * @param int $max_reconnect_attempts 0 = retry forever
	 * @param array<string, mixed>|null $ssl PHP SSL context options, or null for a plain connection
	 */
	public function __construct(
		readonly public string $host = "127.0.0.1",
		readonly public int $port = 5672,
		readonly public string $vhost = "/",
		readonly public string $username = "guest",
		readonly public string $password = "guest",
		readonly public float $heartbeat = 30.0,
		readonly public float $connection_timeout = 10.0,
		readonly public float $poll_interval = 0.025,
		readonly public bool $reconnect = true,
		readonly public float $reconnect_interval = 5.0,
		readonly public int $max_reconnect_attempts = 0,
		readonly public ?array $ssl = null
	){}

	public function description() : string{
		return "amqp" . ($this->ssl !== null ? "s" : "") . "://{$this->username}@{$this->host}:{$this->port}{$this->vhost}";
	}
}
