<?php

declare(strict_types=1);

namespace kostamax27\librabbitmp;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use pocketmine\plugin\PluginBase;
use Symfony\Component\Filesystem\Path;
use function class_exists;
use function dirname;
use function is_file;

final class librabbitmp{
	/**
	 * Creates a RabbitMQ connection for the given plugin. This call blocks the
	 * main thread until the first connection attempt completes, so bad
	 * configuration (wrong host, bad credentials) fails right here instead of
	 * surfacing later in an unrelated operation. With default settings this
	 * takes at most {@see ConnectionSettings::$connection_timeout} seconds.
	 *
	 * <code>
	 * protected function onEnable() : void{
	 *     $this->rabbit = librabbitmp::create($this, ConnectionSettings::fromArray($this->getConfig()->get("rabbitmq")));
	 * }
	 *
	 * protected function onDisable() : void{
	 *     $this->rabbit->waitAll();
	 *     $this->rabbit->close();
	 * }
	 * </code>
	 *
	 * @throws \InvalidArgumentException if the plugin is disabled
	 * @throws \LogicException if the php-amqplib driver cannot be located
	 * @throws ConnectionException if the first connection attempt fails
	 */
	public static function create(PluginBase $plugin, ConnectionSettings $settings) : RabbitMQ{
		$plugin->isEnabled() || throw new \InvalidArgumentException("Cannot create a RabbitMQ connection for a disabled plugin");
		$connection = new RabbitMQ($plugin, $settings, self::locateWorkerAutoloader());
		$error = $connection->thread->waitForFirstConnection();
		if($error !== null){
			$connection->close();
			throw new ConnectionException("Failed to connect to RabbitMQ ({$settings->description()}): {$error}");
		}
		$plugin->getLogger()->debug("Connected to RabbitMQ ({$settings->description()})");
		return $connection;
	}

	/**
	 * @throws \LogicException :(
	 */
	private static function locateWorkerAutoloader() : ?string{
		$virion_root = dirname(__DIR__, 3);
		$autoloader = Path::join($virion_root, "vendor", "autoload.php");
		if(is_file($autoloader)){
			return $autoloader;
		}
		if(class_exists(AMQPStreamConnection::class)){
			return null;
		}
		throw new \LogicException(
			"The php-amqplib driver could not be located: there is no vendor/autoload.php in \"{$virion_root}\" " .
			"and " . AMQPStreamConnection::class . " is not loadable. " .
			"Run \"composer install --no-dev\" in \"{$virion_root}\""
		);
	}

	private function __construct(){}
}
