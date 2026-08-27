<?php

declare(strict_types=1);

namespace kostamax27\librabbitmp\thread;

use kostamax27\librabbitmp\ConnectionException;
use pmmp\thread\ThreadSafe;
use pmmp\thread\ThreadSafeArray;
use function serialize;

/**
 * @internal
 */
final class CommandQueue extends ThreadSafe{
	private bool $invalidated = false;

	/** @var ThreadSafeArray<int, string> */
	private ThreadSafeArray $commands;

	public function __construct(){
		$this->commands = new ThreadSafeArray();
	}

	/**
	 * @throws ConnectionException
	 */
	public function schedule(int $command_id, Opcode $opcode, array $params) : void{
		$row = serialize([$command_id, $opcode->value, $params]);
		$this->synchronized(function() use ($row) : void{
			$this->invalidated && throw new ConnectionException("Cannot schedule a command on a closed connection");
			$this->commands[] = $row;
			$this->notify();
		});
	}

	public function fetch() : ?string{
		return $this->synchronized($this->commands->shift(...));
	}

	public function waitFetch(int $timeout) : ?string{
		return $this->synchronized(function() use ($timeout) : ?string{
			if($this->commands->count() === 0 && !$this->invalidated){
				$this->wait($timeout);
			}
			return $this->commands->shift();
		});
	}

	public function invalidate() : void{
		$this->synchronized(function() : void{
			$this->invalidated = true;
			$this->notify();
		});
	}

	public function isInvalidated() : bool{
		return $this->synchronized(fn() : bool => $this->invalidated);
	}
}
