<?php

declare(strict_types=1);

namespace kostamax27\librabbitmp\thread;

use kostamax27\librabbitmp\ConnectionSettings;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use pmmp\thread\Thread as NativeThread;
use pocketmine\snooze\SleeperHandlerEntry;
use pocketmine\snooze\SleeperNotifier;
use pocketmine\thread\log\ThreadSafeLogger;
use pocketmine\thread\Thread;
use function get_debug_type;
use function is_array;
use function max;
use function min;
use function serialize;
use function sprintf;
use function stream_context_create;
use function unserialize;

//TODO:
//You'll see a lot of `\Throwable` here.
//The problem is that `php-amqplib` doesn't always specify exactly which exceptions are thrown, so they end up declaring `\Exception` instead :(

/**
 * @internal
 */
final class RabbitMQThread extends Thread{
	private const RECONNECT_WAIT = 100_000;
	private const FIRST_CONNECT_WAIT = 500_000;

	private string $settings_serialized;
	private bool $connection_attempted = false;
	private ?string $connection_error = null;

	public function __construct(
		readonly private CommandQueue $commands,
		readonly private EventQueue $events,
		readonly private SleeperHandlerEntry $sleeper_entry,
		readonly private ThreadSafeLogger $logger,
		ConnectionSettings $settings,
		readonly private string $worker_autoloader
	){
		$this->settings_serialized = serialize($settings);
		$this->start(NativeThread::INHERIT_INI);
	}

	public function waitForFirstConnection() : ?string{
		while(true){
			$result = $this->synchronized(function() : array{
				if(!$this->connection_attempted && !$this->isTerminated()){
					$this->wait(self::FIRST_CONNECT_WAIT);
				}
				return [$this->connection_attempted, $this->connection_error];
			});
			[$attempted, $error] = $result;
			if($attempted){
				return $error;
			}
			if($this->isTerminated()){
				return "The worker thread terminated before completing the connection attempt";
			}
		}
	}

	protected function onRun() : void{
		require_once $this->worker_autoloader;

		/** @var ConnectionSettings $settings */
		$settings = unserialize($this->settings_serialized, ["allowed_classes" => [ConnectionSettings::class]]);
		$notifier = $this->sleeper_entry->createNotifier();

		try{
			$session = $this->openSession($settings, generation: 1);
		}catch(\Throwable $e){
			$this->publishConnectionAttempt(self::describe($e));
			return;
		}
		$this->publishConnectionAttempt(null);

		while(true){
			try{
				if($this->processCommands($session, $notifier)){
					break;
				}
				try{
					$session->channel->wait(null, false, $settings->poll_interval);
				}catch(AMQPTimeoutException){
					//no frames within the poll interval - go check the command queue
				}
			}catch(AMQPProtocolChannelException $e){
				$this->logger->warning("[librabbitmp] AMQP channel error: {$e->getMessage()} - reopening channel");
				try{
					$this->recoverChannel($session, $notifier);
					continue;
				}catch(\Throwable $recovery_error){
					$e = $recovery_error;
				}
				if(!$this->handleConnectionLoss($settings, $session, $e, $notifier)){
					return;
				}
			}catch(\Throwable $e){
				if(!$this->handleConnectionLoss($settings, $session, $e, $notifier)){
					return;
				}
			}
		}

		$this->shutdownGracefully($session);
	}

	private static function describe(\Throwable $t) : string{
		return $t->getMessage() !== "" ? $t->getMessage() : get_debug_type($t);
	}

	private function publishConnectionAttempt(?string $error) : void{
		$this->synchronized(function() use ($error) : void{
			$this->connection_error = $error;
			$this->connection_attempted = true;
			$this->notify();
		});
	}

	private function openSession(ConnectionSettings $settings, int $generation) : WorkerSession{
		$connection = new AMQPStreamConnection(
			$settings->host,
			$settings->port,
			$settings->username,
			$settings->password,
			$settings->vhost,
			false,
			"AMQPLAIN",
			null,
			"en_US",
			$settings->connection_timeout,
			max($settings->connection_timeout, $settings->heartbeat * 2), //read_write_timeout must be >= 2x heartbeat
			$settings->ssl !== null ? stream_context_create(["ssl" => $settings->ssl]) : null,
			false,
			(int) $settings->heartbeat,
			$settings->connection_timeout //channel_rpc_timeout
		);
		return new WorkerSession($connection, $connection->channel(), $generation);
	}

	private function processCommands(WorkerSession $session, SleeperNotifier $notifier) : bool{
		while(($row = $this->commands->fetch()) !== null){
			[$command_id, $opcode_raw, $params] = self::unserializeCommand($row);
			$opcode = Opcode::from($opcode_raw);
			try{
				$result = $this->execute($session, $notifier, $opcode, $params);
				$this->events->publish(EventType::RESPONSE_OK, [$command_id, $result]);
				$notifier->wakeupSleeper();
			}catch(AMQPChannelClosedException $e){
				//operation-level failure: reject only this command, then heal
				//the channel so subsequent commands keep working.
				$this->events->publish(EventType::RESPONSE_ERROR, [$command_id, self::describe($e), $e->getCode(), false]);
				$notifier->wakeupSleeper();
				$this->recoverChannel($session, $notifier);
			}catch(\Throwable $e){
				$this->events->publish(EventType::RESPONSE_ERROR, [$command_id, self::describe($e), 0, false]);
				$notifier->wakeupSleeper();
				throw $e;
			}
		}
		return $this->commands->isInvalidated();
	}

	/**
	 * @return array{int, int, array<int, mixed>}
	 */
	private static function unserializeCommand(string $row) : array{
		return unserialize($row, ["allowed_classes" => [\DateTime::class, \DateTimeImmutable::class]]);
	}

	/**
	 * @return array<int|string, mixed>|string|int|null
	 */
	private function execute(WorkerSession $session, SleeperNotifier $notifier, Opcode $opcode, array $params) : array|string|int|null{
		$channel = $session->channel;
		switch($opcode){
			case Opcode::EXCHANGE_DECLARE:
				/** @var array{string, string, bool, bool, bool, bool, array<string, mixed>} $params */
				$channel->exchange_declare($params[0], $params[1], $params[2], $params[3], $params[4], $params[5], false, $params[6]);
				return null;
			case Opcode::EXCHANGE_DELETE:
				/** @var array{string, bool} $params */
				$channel->exchange_delete($params[0], $params[1]);
				return null;
			case Opcode::QUEUE_DECLARE:
				/** @var array{string, bool, bool, bool, bool, array<string, mixed>} $params */
				$result = $channel->queue_declare($params[0], $params[1], $params[2], $params[3], $params[4], false, $params[5]);
				is_array($result) || throw new \LogicException("Expected a queue.declare-ok result, got " . get_debug_type($result));
				return [(string) $result[0], (int) $result[1], (int) $result[2]];
			case Opcode::QUEUE_BIND:
				/** @var array{string, string, string, array<string, mixed>} $params */
				$channel->queue_bind($params[0], $params[1], $params[2], false, $params[3]);
				return null;
			case Opcode::QUEUE_UNBIND:
				/** @var array{string, string, string, array<string, mixed>} $params */
				$channel->queue_unbind($params[0], $params[1], $params[2], $params[3]);
				return null;
			case Opcode::QUEUE_DELETE:
				/** @var array{string, bool, bool} $params */
				return (int) $channel->queue_delete($params[0], $params[1], $params[2]);
			case Opcode::QUEUE_PURGE:
				/** @var array{string} $params */
				return (int) $channel->queue_purge($params[0]);
			case Opcode::PUBLISH:
				/** @var array{string, array<string, mixed>, string, string} $params */
				[$body, $properties, $exchange, $routing_key] = $params;
				if(isset($properties["application_headers"]) && is_array($properties["application_headers"])){
					$properties["application_headers"] = new AMQPTable($properties["application_headers"]);
				}
				$channel->basic_publish(new AMQPMessage($body, $properties), $exchange, $routing_key);
				return null;
			case Opcode::CONSUME:
				/** @var array{string, string, bool, bool, array<string, mixed>} $params */
				[$queue, $consumer_tag, $no_ack, $exclusive, $arguments] = $params;
				$tag = $this->registerConsumer($session, $notifier, $consumer_tag, $queue, $no_ack, $exclusive, $arguments);
				$session->consumers[$tag] = ["queue" => $queue, "no_ack" => $no_ack, "exclusive" => $exclusive, "arguments" => $arguments, "topic" => null];
				return $tag;
			case Opcode::SUBSCRIBE_TOPIC:
				/** @var array{string, string, string} $params */
				[$exchange, $pattern, $consumer_tag] = $params;
				$queue = $this->bindTopicQueue($session, $exchange, $pattern);
				$tag = $this->registerConsumer($session, $notifier, $consumer_tag, $queue, no_ack: true, exclusive: false, arguments: []);
				$session->consumers[$tag] = ["queue" => $queue, "no_ack" => true, "exclusive" => false, "arguments" => [], "topic" => [$exchange, $pattern]];
				return [$tag, $queue];
			case Opcode::CANCEL:
				/** @var array{string} $params */
				unset($session->consumers[$params[0]]);
				$channel->basic_cancel($params[0]);
				return null;
			case Opcode::GET:
				/** @var array{string, bool} $params */
				$message = $channel->basic_get($params[0], $params[1]);
				if($message === null){
					return null;
				}
				return self::encodeMessage($message, $session->generation);
			case Opcode::ACK:
				/** @var array{int, int, bool} $params */
				[$delivery_tag, $generation, $multiple] = $params;
				if($generation !== $session->generation){
					$this->logger->debug("[librabbitmp] Ignoring ack for stale delivery tag {$delivery_tag} (connection was re-established)");
					return null;
				}
				$session->channel->basic_ack($delivery_tag, $multiple);
				return null;
			case Opcode::NACK:
				/** @var array{int, int, bool, bool} $params */
				[$delivery_tag, $generation, $multiple, $requeue] = $params;
				if($generation !== $session->generation){
					$this->logger->debug("[librabbitmp] Ignoring nack for stale delivery tag {$delivery_tag} (connection was re-established)");
					return null;
				}
				$session->channel->basic_nack($delivery_tag, $multiple, $requeue);
				return null;
			case Opcode::QOS:
				/** @var array{int, int, bool} $params */
				$channel->basic_qos($params[0], $params[1], $params[2]);
				$session->qos = [$params[0], $params[1], $params[2]];
				return null;
		}
		throw new \LogicException("Unhandled opcode {$opcode->name}");
	}

	private function bindTopicQueue(WorkerSession $session, string $exchange, string $pattern) : string{
		$result = $session->channel->queue_declare("", false, false, true, true, false, []);
		is_array($result) || throw new \LogicException("Expected a queue.declare-ok result, got " . get_debug_type($result));
		$queue = (string) $result[0];
		$session->channel->queue_bind($queue, $exchange, $pattern, false, []);
		return $queue;
	}

	private function registerConsumer(WorkerSession $session, SleeperNotifier $notifier, string $consumer_tag, string $queue, bool $no_ack, bool $exclusive, array $arguments) : string{
		$events = $this->events;
		$callback = static function(AMQPMessage $message) use ($events, $notifier, $session) : void{
			$events->publish(EventType::DELIVERY, self::encodeMessage($message, $session->generation));
			$notifier->wakeupSleeper();
		};
		return $session->channel->basic_consume($queue, $consumer_tag, false, $no_ack, $exclusive, false, $callback, null, $arguments);
	}

	/**
	 * @return array{string|null, int, bool, string, string, array<string, mixed>, string, int}
	 */
	private static function encodeMessage(AMQPMessage $message, int $generation) : array{
		//AMQP basic properties keep php-amqplib's names ("reply_to",
		//"correlation_id", "delivery_mode", ...); custom application headers
		//are merged in flat, with properties winning on a name collision.
		$headers = $message->get_properties();
		$application_headers = $headers["application_headers"] ?? null;
		unset($headers["application_headers"]);
		if($application_headers instanceof AMQPTable){
			$application_headers = $application_headers->getNativeData();
		}
		if(is_array($application_headers)){
			$headers += $application_headers;
		}
		return [
			$message->getConsumerTag(),
			(int) $message->getDeliveryTag(),
			(bool) $message->isRedelivered(),
			(string) $message->getExchange(),
			(string) $message->getRoutingKey(),
			$headers,
			$message->getBody(),
			$generation,
		];
	}

	private function recoverChannel(WorkerSession $session, SleeperNotifier $notifier) : void{
		//php-amqplib already replied with channel.close-ok and marked the
		//channel closed before throwing AMQPProtocolChannelException, so a
		//fresh channel is all that is needed.
		$this->replaceChannel($session);
		$this->restoreChannelState($session, $notifier);
	}

	private function replaceChannel(WorkerSession $session) : void{
		try{
			$session->channel->close();
		}catch(\Throwable $e){
			//already closed by the broker, or the close handshake failed - either way it is unusable
		}
		$session->channel = $session->connection->channel();
		$session->generation++;
	}

	private function restoreChannelState(WorkerSession $session, SleeperNotifier $notifier) : void{
		$restored = [];
		while(true){
			$current_tag = null;
			try{
				if($session->qos !== null){
					$session->channel->basic_qos($session->qos[0], $session->qos[1], $session->qos[2]);
				}
				foreach($session->consumers as $tag => $consumer){
					if(isset($restored[$tag])){
						continue;
					}
					$current_tag = $tag;
					if($consumer["topic"] !== null){
						[$exchange, $pattern] = $consumer["topic"];
						$consumer["queue"] = $this->bindTopicQueue($session, $exchange, $pattern);
						$session->consumers[$tag]["queue"] = $consumer["queue"];
					}
					$this->registerConsumer($session, $notifier, $tag, $consumer["queue"], $consumer["no_ack"], $consumer["exclusive"], $consumer["arguments"]);
					$restored[$tag] = true;
				}
				return;
			}catch(AMQPChannelClosedException $e){
				if($current_tag === null){
					throw $e;
				}
				$reason = self::describe($e);
				$this->logger->warning("[librabbitmp] Dropping consumer \"{$current_tag}\": failed to restore it after recovery: {$reason}");
				unset($session->consumers[$current_tag]);
				$this->events->publish(EventType::CONSUMER_LOST, [$current_tag, $reason]);
				$notifier->wakeupSleeper();
				$this->replaceChannel($session);
				$restored = [];
			}
		}
	}

	private function handleConnectionLoss(ConnectionSettings $settings, WorkerSession $session, \Throwable $error, SleeperNotifier $notifier) : bool{
		$reason = self::describe($error);
		$can_reconnect = $settings->reconnect && !$this->commands->isInvalidated();
		$this->logger->error("[librabbitmp] Lost connection to RabbitMQ broker: {$reason}" . ($can_reconnect ? " - reconnecting" : ""));
		$this->events->publish(EventType::CONNECTION_LOST, [$reason, !$can_reconnect]);
		$notifier->wakeupSleeper();
		self::disposeConnection($session);

		if(!$can_reconnect){
			return false;
		}

		$attempts = 0;
		while(!$this->commands->isInvalidated()){
			$attempts++;
			if($settings->max_reconnect_attempts > 0 && $attempts > $settings->max_reconnect_attempts){
				$this->logger->error("[librabbitmp] Giving up reconnecting to RabbitMQ broker after {$settings->max_reconnect_attempts} failed attempts");
				$this->events->publish(EventType::CONNECTION_LOST, ["Gave up after {$settings->max_reconnect_attempts} failed reconnection attempts (last error: {$reason})", true]);
				$notifier->wakeupSleeper();
				return false;
			}
			try{
				$replacement = $this->openSession($settings, $session->generation + 1);
			}catch(\Throwable $e){
				$reason = self::describe($e);
				$this->logger->debug("[librabbitmp] Reconnection attempt #{$attempts} failed: {$reason}");
				if(!$this->sleepThroughReconnectDelay($settings, $notifier)){
					return false;
				}
				continue;
			}

			$session->generation = $replacement->generation;
			$session->connection = $replacement->connection;
			$session->channel = $replacement->channel;
			try{
				$this->restoreChannelState($session, $notifier);
			}catch(\Throwable $e){
				$reason = self::describe($e);
				$this->logger->debug("[librabbitmp] Failed to restore consumers after reconnecting: {$reason}");
				self::disposeConnection($session);
				if(!$this->sleepThroughReconnectDelay($settings, $notifier)){
					return false;
				}
				continue;
			}

			$this->logger->info("[librabbitmp] Re-established connection to RabbitMQ broker ({$settings->description()})");
			$this->events->publish(EventType::CONNECTION_RESTORED, []);
			$notifier->wakeupSleeper();
			return true;
		}
		return false;
	}

	private static function disposeConnection(WorkerSession $session) : void{
		try{
			$session->connection->close();
		}catch(\Throwable $e){
			//the socket is most likely already gone
		}
	}

	private function sleepThroughReconnectDelay(ConnectionSettings $settings, SleeperNotifier $notifier) : bool{
		$remaining = max(1, (int) ($settings->reconnect_interval * 1_000_000));
		while($remaining > 0){
			if($this->commands->isInvalidated()){
				return false;
			}
			$slice = min($remaining, self::RECONNECT_WAIT);
			$row = $this->commands->waitFetch($slice);
			if($row !== null){
				[$command_id, $opcode_raw,] = self::unserializeCommand($row);
				$this->logger->debug(sprintf("[librabbitmp] Failing %s command #%d scheduled while reconnecting", Opcode::from($opcode_raw)->operationName(), $command_id));
				$this->events->publish(EventType::RESPONSE_ERROR, [$command_id, "The connection to the RabbitMQ broker is unavailable (reconnecting)", 0, true]);
				$notifier->wakeupSleeper();
				continue;
			}
			$remaining -= $slice;
		}
		return !$this->commands->isInvalidated();
	}

	private function shutdownGracefully(WorkerSession $session) : void{
		try{
			foreach($session->consumers as $tag => $_){
				$session->channel->basic_cancel($tag);
			}
			$session->channel->close();
			$session->connection->close();
		}catch(\Throwable $e){
			$this->logger->debug("[librabbitmp] Error during graceful shutdown: {$e->getMessage()}");
		}
	}

	public function quit() : void{
		$this->commands->invalidate();
		parent::quit();
	}
}
