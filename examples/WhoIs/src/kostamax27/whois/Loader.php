<?php

declare(strict_types=1);

namespace kostamax27\whois;

use kostamax27\librabbitmp\ConnectionSettings;
use kostamax27\librabbitmp\Delivery;
use kostamax27\librabbitmp\librabbitmp;
use kostamax27\librabbitmp\MessageProperties;
use kostamax27\librabbitmp\RabbitMQ;
use kostamax27\librabbitmp\RabbitMQException;
use kostamax27\librabbitmp\Subscription;
use pocketmine\command\Command;
use pocketmine\command\CommandSender;
use pocketmine\command\utils\InvalidCommandSyntaxException;
use pocketmine\player\Player;
use pocketmine\plugin\PluginBase;
use pocketmine\utils\TextFormat;
use SOFe\AwaitGenerator\Await;
use function array_keys;
use function count;
use function implode;
use function is_array;
use function is_string;
use function json_decode;
use function json_encode;
use function strtolower;
use const JSON_THROW_ON_ERROR;

/**
 * /whois <server> <player> asks the named server over RPC whether the player
 * is online there right now, and shows their world and ping. Every server
 * answers requests on its own queue "whois.rpc.<server-name>" and can query
 * any other.
 */
final class Loader extends PluginBase{
	private const TIMEOUT_TICKS = 100;

	private string $server_name;
	private RabbitMQ $rabbit;
	private ?RpcClient $rpc = null;
	private ?Subscription $rpc_server = null;

	protected function onEnable() : void{
		$this->saveDefaultConfig();
		$server_name = $this->getConfig()->get("server-name");
		$this->server_name = is_string($server_name) && $server_name !== "" ? $server_name : "unknown";

		/** @var array<string, mixed> $rabbitmq_config */
		$rabbitmq_config = $this->getConfig()->get("rabbitmq", []);
		$this->rabbit = librabbitmp::create($this, ConnectionSettings::fromArray($rabbitmq_config));

		//the request queue is auto-delete: if the broker restarts, it is gone
		//and the consumer cannot be restored - declare and consume again
		$this->rabbit->onConsumerLost(function(string $consumer_tag) : void{
			if($this->rpc_server !== null && $consumer_tag === $this->rpc_server->consumer_tag){
				$this->rpc_server = null;
				Await::f2c(function() : \Generator{
					try{
						yield from $this->startRpc();
					}catch(RabbitMQException $e){
						$this->getLogger()->logException($e);
					}
				});
			}
		});

		Await::f2c(function() : \Generator{
			try{
				yield from $this->startRpc();
				$rpc = new RpcClient($this, $this->rabbit);
				yield from $rpc->init();
				$this->rpc = $rpc;
			}catch(RabbitMQException $e){
				$this->getLogger()->logException($e);
			}
		});
	}

	protected function onDisable() : void{
		if(isset($this->rabbit)){
			$this->rabbit->waitAll();
			$this->rabbit->close();
		}
	}

	private static function queueName(string $server) : string{
		return "whois.rpc." . strtolower($server);
	}

	/**
	 * @return \Generator<mixed, Await::RESOLVE|Await::REJECT, mixed, void>
	 */
	private function startRpc() : \Generator{
		yield from $this->rabbit->queueDeclare(self::queueName($this->server_name), durable: true, auto_delete: true);
		$this->rpc_server = yield from $this->rabbit->consume(self::queueName($this->server_name), $this->onRequest(...), no_ack: true);
	}

	private function onRequest(Delivery $request) : void{
		$reply_to = $request->getHeader("reply_to");
		$correlation_id = $request->getHeader("correlation_id");
		if(!is_string($reply_to) || !is_string($correlation_id)){
			$this->getLogger()->debug("Ignoring request without reply_to/correlation_id (headers: " . implode(", ", array_keys($request->headers)) . ")");
			return;
		}
		$payload = json_decode($request->body, true);
		$name = is_array($payload) && is_string($payload["player"] ?? null) ? $payload["player"] : "";
		$player = $this->getServer()->getPlayerExact($name);
		$reply = json_encode($player === null ? ["online" => false] : [
			"online" => true,
			"name" => $player->getName(),
			"world" => $player->getWorld()->getDisplayName(),
			"ping" => $player->getNetworkSession()->getPing(),
		], JSON_THROW_ON_ERROR);
		Await::f2c(function() use ($reply, $correlation_id, $reply_to) : \Generator{
			try{
				yield from $this->rabbit->publish($reply, MessageProperties::json()->correlationId($correlation_id), "", $reply_to);
			}catch(RabbitMQException){
				$this->getLogger()->debug("Failed to publish a /whois reply");
			}
		});
	}

	public function onCommand(CommandSender $sender, Command $command, string $label, array $args) : bool{
		if(count($args) !== 2){
			throw new InvalidCommandSyntaxException();
		}
		if($this->rpc === null){
			$sender->sendMessage(TextFormat::RED . "Still connecting, try again in a moment.");
			return true;
		}
		[$server, $player] = $args;
		Await::f2c(function() use ($sender, $server, $player) : \Generator{
			try{
				$reply = yield from $this->rpc->call(self::queueName($server), json_encode(["player" => $player], JSON_THROW_ON_ERROR), MessageProperties::json(), self::TIMEOUT_TICKS);
			}catch(RpcTimeoutException){
				$this->sendTo($sender, TextFormat::RED . "\"{$server}\" did not respond - is it online?");
				return;
			}catch(RabbitMQException $e){
				$this->getLogger()->logException($e);
				$this->sendTo($sender, TextFormat::RED . "Lookup failed, please try again later.");
				return;
			}
			$info = json_decode($reply, true);
			if(!is_array($info)){
				return;
			}
			$this->sendTo($sender, ($info["online"] ?? false) !== true
				? TextFormat::YELLOW . "{$player} is not online on \"{$server}\""
				: TextFormat::GREEN . "{$info["name"]} is on \"{$server}\" in world \"{$info["world"]}\" ({$info["ping"]}ms)");
		});
		return true;
	}

	private function sendTo(CommandSender $sender, string $message) : void{
		if($sender instanceof Player && !$sender->isConnected()){
			return;
		}
		$sender->sendMessage($message);
	}
}
