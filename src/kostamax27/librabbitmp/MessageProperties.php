<?php

declare(strict_types=1);

namespace kostamax27\librabbitmp;

use PhpAmqpLib\Message\AMQPMessage;

/**
 * Typed builder for the AMQP basic properties of a published message, so
 * plugin code never hand-writes driver arrays like ["delivery_mode" => 2].
 * Only properties that were explicitly set are sent; everything else keeps
 * the AMQP defaults.
 *
 * <code>
 * yield from $rabbit->publish(
 *     body: json_encode($payload),
 *     properties: MessageProperties::json()->persistent(),
 *     routing_key: "jobs"
 * );
 * </code>
 *
 * @see RabbitMQ::publish()
 */
final class MessageProperties{
	public static function create() : self{
		return new self();
	}

	/** Shortcut for {@see MessageProperties::contentType()} with "application/json". */
	public static function json() : self{
		return (new self())->contentType("application/json");
	}

	/** Shortcut for {@see MessageProperties::contentType()} with "text/plain". */
	public static function text() : self{
		return (new self())->contentType("text/plain");
	}

	private ?string $content_type = null;
	private ?string $content_encoding = null;
	private ?DeliveryMode $delivery_mode = null;
	private ?int $priority = null;
	private ?string $correlation_id = null;
	private ?string $reply_to = null;
	private ?int $expiration_millis = null;
	private ?string $message_id = null;
	private ?\DateTimeImmutable $timestamp = null;
	private ?string $type = null;
	private ?string $user_id = null;
	private ?string $app_id = null;

	/** @var array<string, mixed> */
	private array $headers = [];

	private function __construct(){}

	/** MIME type of the body, e.g. "application/json". Informational for consumers - the broker does not interpret it. */
	public function contentType(string $mime_type) : self{
		$this->content_type = $mime_type;
		return $this;
	}

	/** Body encoding, e.g. "gzip". Informational for consumers, like Content-Encoding in HTTP. */
	public function contentEncoding(string $encoding) : self{
		$this->content_encoding = $encoding;
		return $this;
	}

	public function deliveryMode(DeliveryMode $mode) : self{
		$this->delivery_mode = $mode;
		return $this;
	}

	/** Shortcut for {@see MessageProperties::deliveryMode()} with {@see DeliveryMode::PERSISTENT}. */
	public function persistent() : self{
		return $this->deliveryMode(DeliveryMode::PERSISTENT);
	}

	/**
	 * Message priority. Only honoured by queues declared with the
	 * "x-max-priority" argument; RabbitMQ recommends values between 1 and 10.
	 */
	public function priority(int $priority) : self{
		($priority >= 0 && $priority <= 255) || throw new \InvalidArgumentException("Message priority must be in [0, 255], got {$priority}");
		$this->priority = $priority;
		return $this;
	}

	/** Application-defined id correlating a response to its request (RPC pattern, together with {@see MessageProperties::replyTo()}). */
	public function correlationId(string $correlation_id) : self{
		$this->correlation_id = $correlation_id;
		return $this;
	}

	/** Name of the queue the consumer should publish its response to (RPC pattern). */
	public function replyTo(string $queue) : self{
		$this->reply_to = $queue;
		return $this;
	}

	/** Per-message TTL: the broker discards (or dead-letters) the message if it stays queued longer than this. */
	public function expiresAfterMillis(int $millis) : self{
		$millis >= 0 || throw new \InvalidArgumentException("Message expiration must be >= 0, got {$millis}");
		$this->expiration_millis = $millis;
		return $this;
	}

	/** Application-defined message id, e.g. for deduplication on the consumer side. */
	public function messageId(string $message_id) : self{
		$this->message_id = $message_id;
		return $this;
	}

	/** Application-defined creation time of the message. */
	public function timestamp(\DateTimeInterface $timestamp) : self{
		$this->timestamp = \DateTimeImmutable::createFromInterface($timestamp);
		return $this;
	}

	/** Application-defined message type name, e.g. "player.join". */
	public function type(string $type) : self{
		$this->type = $type;
		return $this;
	}

	/** Publishing user id. RabbitMQ validates it against the authenticated connection user and rejects mismatches. */
	public function userId(string $user_id) : self{
		$this->user_id = $user_id;
		return $this;
	}

	/** Application-defined publishing application id. */
	public function appId(string $app_id) : self{
		$this->app_id = $app_id;
		return $this;
	}

	/**
	 * Sets one custom application header. Values must survive serialize()
	 * (scalars, arrays, null, DateTime / \DateTimeImmutable).
	 */
	public function header(string $name, mixed $value) : self{
		$this->headers[$name] = $value;
		return $this;
	}

	/**
	 * @internal flattens the builder into the property array
	 * {@see AMQPMessage} expects. Unset properties are
	 * omitted so AMQP defaults apply.
	 *
	 * @return array<string, mixed>
	 */
	public function toDriverHeaders() : array{
		$out = [];
		if($this->content_type !== null){
			$out["content_type"] = $this->content_type;
		}
		if($this->content_encoding !== null){
			$out["content_encoding"] = $this->content_encoding;
		}
		if($this->delivery_mode !== null){
			$out["delivery_mode"] = $this->delivery_mode->value;
		}
		if($this->priority !== null){
			$out["priority"] = $this->priority;
		}
		if($this->correlation_id !== null){
			$out["correlation_id"] = $this->correlation_id;
		}
		if($this->reply_to !== null){
			$out["reply_to"] = $this->reply_to;
		}
		if($this->expiration_millis !== null){
			$out["expiration"] = (string) $this->expiration_millis; //AMQP encodes TTL as a shortstr of milliseconds
		}
		if($this->message_id !== null){
			$out["message_id"] = $this->message_id;
		}
		if($this->timestamp !== null){
			$out["timestamp"] = $this->timestamp->getTimestamp();
		}
		if($this->type !== null){
			$out["type"] = $this->type;
		}
		if($this->user_id !== null){
			$out["user_id"] = $this->user_id;
		}
		if($this->app_id !== null){
			$out["app_id"] = $this->app_id;
		}
		if($this->headers !== []){
			$out["application_headers"] = $this->headers;
		}
		return $out;
	}
}
