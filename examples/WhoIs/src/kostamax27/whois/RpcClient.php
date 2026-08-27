<?php

declare(strict_types=1);

namespace kostamax27\whois;

use kostamax27\librabbitmp\ConnectionException;
use kostamax27\librabbitmp\Delivery;
use kostamax27\librabbitmp\MessageProperties;
use kostamax27\librabbitmp\OperationException;
use kostamax27\librabbitmp\RabbitMQ;
use pocketmine\plugin\PluginBase;
use pocketmine\scheduler\ClosureTask;
use SOFe\AwaitGenerator\Await;
use function array_keys;
use function implode;
use function is_string;
use function var_export;

/**
 * RPC over RabbitMQ direct reply-to: requests are published to the callee's
 * queue with a "reply-to" of the pseudo-queue "amq.rabbitmq.reply-to", replies
 * come back to this connection's consumer on it and are matched to the caller
 * by "correlation_id". No reply queues to declare or clean up.
 */
final class RpcClient{
	private const REPLY_QUEUE = "amq.rabbitmq.reply-to";

	private int $next_correlation_id = 0;

	/** @var array<string, array{\Closure(string) : void, \Closure(RpcTimeoutException|ConnectionException) : void}> */
	private array $pending = [];

	public function __construct(
		readonly private PluginBase $plugin,
		readonly private RabbitMQ $connection
	){
		$connection->onConnectionLost(function(ConnectionException $error) : void{
			$pending = $this->pending;
			$this->pending = [];
			foreach($pending as [, $reject]){
				$reject($error);
			}
		});
	}

	/**
	 * Must complete before {@see RpcClient::call()} - the broker rejects
	 * publishing with a direct reply-to address unless the connection is
	 * already consuming from it.
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, void>
	 */
	public function init() : \Generator{
		yield from $this->connection->consume(self::REPLY_QUEUE, $this->onReply(...), no_ack: true);
	}

	/**
	 * Publishes $body to $queue and resolves with the reply body.
	 *
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, string>
	 * @throws RpcTimeoutException if no reply arrived within $timeout_ticks
	 * @throws ConnectionException if the connection is lost or closed while waiting
	 * @throws OperationException if the broker rejects the request publish
	 */
	public function call(string $queue, string $body, MessageProperties $properties, int $timeout_ticks) : \Generator{
		$correlation_id = (string) $this->next_correlation_id++;
		$properties->replyTo(self::REPLY_QUEUE)->correlationId($correlation_id);
		//expire the request in the broker once the caller stops waiting for
		//it - a callee coming online later must not answer stale requests
		$properties->expiresAfterMillis($timeout_ticks * 50);
		yield from $this->connection->publish($body, $properties, "", $queue);
		//TODO: HACK! See -> https://github.com/SOF3/await-generator/issues/212
		$timeout_task = null;
		try{
			return yield from Await::promise(function(\Closure $resolve, \Closure $reject) use ($correlation_id, $queue, $timeout_ticks, &$timeout_task) : void{
				$this->pending[$correlation_id] = [$resolve, $reject];
				$timeout_task = $this->plugin->getScheduler()->scheduleDelayedTask(new ClosureTask(function() use ($correlation_id, $queue, $timeout_ticks, $reject) : void{
					if(isset($this->pending[$correlation_id])){
						unset($this->pending[$correlation_id]);
						$reject(new RpcTimeoutException("No reply from \"{$queue}\" within " . ($timeout_ticks * 50) . "ms"));
					}
				}), $timeout_ticks);
			});
		}finally{
			if($timeout_task !== null && !$timeout_task->isCancelled()){
				$timeout_task->cancel();
			}
		}
	}

	private function onReply(Delivery $delivery) : void{
		$correlation_id = $delivery->getHeader("correlation_id");
		if(is_string($correlation_id) && isset($this->pending[$correlation_id])){
			[$resolve,] = $this->pending[$correlation_id];
			unset($this->pending[$correlation_id]);
			$resolve($delivery->body);
		}else{
			$this->plugin->getLogger()->debug("Unmatched RPC reply (correlation_id: " . var_export($correlation_id, true) . ", headers: " . implode(", ", array_keys($delivery->headers)) . ")");
		}
	}
}
