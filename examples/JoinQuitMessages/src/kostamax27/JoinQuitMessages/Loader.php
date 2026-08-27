<?php

declare(strict_types=1);

namespace kostamax27\JoinQuitMessages;

use kostamax27\librabbitmp\ConnectionException;
use kostamax27\librabbitmp\ConnectionSettings;
use kostamax27\librabbitmp\Delivery;
use kostamax27\librabbitmp\librabbitmp;
use kostamax27\librabbitmp\RabbitMQ;
use kostamax27\librabbitmp\RabbitMQException;
use pocketmine\plugin\PluginBase;
use pocketmine\utils\TextFormat;
use SOFe\AwaitGenerator\Await;
use function is_array;
use function is_string;
use function json_decode;

/**
 * Publishes join/quit events to the "network" topic exchange (routing keys
 * "player.join" / "player.quit") and subscribes to "player.*" to display
 * everyone else's.
 */
final class Loader extends PluginBase{
	private string $server_name;
	private RabbitMQ $rabbit;

	protected function onEnable() : void{
		$this->saveDefaultConfig();
		$server_name = $this->getConfig()->get("server-name");
		$this->server_name = is_string($server_name) && $server_name !== "" ? $server_name : "unknown";

		/** @var array<string, mixed> $rabbitmq_config */
		$rabbitmq_config = $this->getConfig()->get("rabbitmq", []);
		$this->rabbit = librabbitmp::create($this, ConnectionSettings::fromArray($rabbitmq_config));

		$this->rabbit->onConnectionLost(function(ConnectionException $error, bool $permanent) : void{
			$this->getLogger()->warning("Network messages are paused: {$error->getMessage()}");
		});
		$this->rabbit->onConnectionRestored(function() : void{
			$this->getLogger()->notice("Network messages resumed");
		});

		Await::f2c(function() : \Generator{
			try{
				$topic = yield from $this->rabbit->topic("network");
				$this->getServer()->getPluginManager()->registerEvents(new EventListener($this, $topic, $this->server_name), $this);
				$events = $topic->messages("player.*");
				while(yield from $events->next($delivery)){
					$this->onRemoteEvent($delivery);
				}
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

	private function onRemoteEvent(Delivery $delivery) : void{
		$payload = json_decode($delivery->body, true);
		if(!is_array($payload) || !is_string($payload["server"] ?? null) || !is_string($payload["player"] ?? null)){
			$this->getLogger()->debug("Ignoring malformed network event: {$delivery->body}");
			return;
		}
		if($payload["server"] === $this->server_name){
			return;
		}
		$message = match($delivery->routing_key){
			"player.join" => TextFormat::YELLOW . "{$payload["player"]} joined the game on {$payload["server"]}",
			"player.quit" => TextFormat::YELLOW . "{$payload["player"]} left the game on {$payload["server"]}",
			default => null
		};
		if($message !== null){
			$this->getServer()->broadcastMessage($message);
		}
	}
}
