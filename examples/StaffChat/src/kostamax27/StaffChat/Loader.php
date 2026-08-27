<?php

declare(strict_types=1);

namespace kostamax27\StaffChat;

use kostamax27\librabbitmp\ConnectionSettings;
use kostamax27\librabbitmp\Delivery;
use kostamax27\librabbitmp\librabbitmp;
use kostamax27\librabbitmp\RabbitMQ;
use kostamax27\librabbitmp\RabbitMQException;
use kostamax27\librabbitmp\Topic;
use pocketmine\command\Command;
use pocketmine\command\CommandSender;
use pocketmine\command\utils\InvalidCommandSyntaxException;
use pocketmine\plugin\PluginBase;
use pocketmine\utils\TextFormat;
use SOFe\AwaitGenerator\Await;
use function count;
use function implode;
use function is_array;
use function is_string;
use function json_decode;
use function json_encode;
use function strtr;
use const JSON_THROW_ON_ERROR;

/**
 * Cross-server staff chat: /sc <message> publishes to the "staffchat" topic
 * exchange with routing key "message.<server>"; every subscribed server
 * (including this one) displays it.
 */
final class Loader extends PluginBase{
	private const FORMAT = TextFormat::BLUE . "[StaffChat]" . TextFormat::GRAY . " [{server}] " . TextFormat::WHITE . "{author}" . TextFormat::GRAY . ": {message}";

	private string $server_name;
	private RabbitMQ $rabbit;
	private ?Topic $topic = null;

	protected function onEnable() : void{
		$this->saveDefaultConfig();
		$server_name = $this->getConfig()->get("server-name");
		$this->server_name = is_string($server_name) && $server_name !== "" ? $server_name : "unknown";

		/** @var array<string, mixed> $rabbitmq_config */
		$rabbitmq_config = $this->getConfig()->get("rabbitmq", []);
		$this->rabbit = librabbitmp::create($this, ConnectionSettings::fromArray($rabbitmq_config));

		Await::f2c(function() : \Generator{
			try{
				$topic = yield from $this->rabbit->topic("staffchat");
				yield from $topic->subscribe("message.*", $this->onStaffMessage(...));
				$this->topic = $topic;
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

	public function onCommand(CommandSender $sender, Command $command, string $label, array $args) : bool{
		if(count($args) === 0){
			throw new InvalidCommandSyntaxException();
		}
		if($this->topic === null){
			$sender->sendMessage(TextFormat::RED . "Staff chat is still connecting, try again in a moment.");
			return true;
		}
		$payload = json_encode(["server" => $this->server_name, "author" => $sender->getName(), "message" => implode(" ", $args)], JSON_THROW_ON_ERROR);
		Await::f2c(function() use ($payload, $sender) : \Generator{
			try{
				yield from $this->topic->publish("message.{$this->server_name}", $payload);
			}catch(RabbitMQException){
				$sender->sendMessage(TextFormat::RED . "Failed to deliver your staff chat message.");
			}
		});
		return true;
	}

	private function onStaffMessage(Delivery $delivery) : void{
		$payload = json_decode($delivery->body, true);
		if(!is_array($payload) || !is_string($payload["server"] ?? null) || !is_string($payload["author"] ?? null) || !is_string($payload["message"] ?? null)){
			$this->getLogger()->debug("Ignoring malformed staff chat message: {$delivery->body}");
			return;
		}
		$message = strtr(self::FORMAT, ["{server}" => $payload["server"], "{author}" => $payload["author"], "{message}" => $payload["message"]]);
		$this->getServer()->getLogger()->info($message);
		foreach($this->getServer()->getOnlinePlayers() as $player){
			if($player->hasPermission("staffchat.use")){
				$player->sendMessage($message);
			}
		}
	}
}
