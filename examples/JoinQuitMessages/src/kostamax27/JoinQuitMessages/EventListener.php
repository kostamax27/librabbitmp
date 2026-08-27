<?php

declare(strict_types=1);

namespace kostamax27\JoinQuitMessages;

use kostamax27\librabbitmp\RabbitMQException;
use kostamax27\librabbitmp\Topic;
use pocketmine\event\Listener;
use pocketmine\event\player\PlayerJoinEvent;
use pocketmine\event\player\PlayerQuitEvent;
use SOFe\AwaitGenerator\Await;
use function json_encode;
use const JSON_THROW_ON_ERROR;

final class EventListener implements Listener{
	public function __construct(
		readonly private Loader $plugin,
		readonly private Topic $topic,
		readonly private string $server_name
	){}

	/**
	 * @priority MONITOR
	 */
	public function onPlayerJoin(PlayerJoinEvent $event) : void{
		$this->publish("player.join", $event->getPlayer()->getName());
	}

	/**
	 * @priority MONITOR
	 */
	public function onPlayerQuit(PlayerQuitEvent $event) : void{
		$this->publish("player.quit", $event->getPlayer()->getName());
	}

	private function publish(string $routing_key, string $player) : void{
		$payload = json_encode(["server" => $this->server_name, "player" => $player], JSON_THROW_ON_ERROR);
		Await::f2c(function() use ($routing_key, $payload) : \Generator{
			try{
				yield from $this->topic->publish($routing_key, $payload);
			}catch(RabbitMQException){
				$this->plugin->getLogger()->debug("Failed to publish {$routing_key} event");
			}
		});
	}
}
