<?php

declare(strict_types=1);

namespace kostamax27\librabbitmp\thread;

use pmmp\thread\ThreadSafe;
use pmmp\thread\ThreadSafeArray;
use function is_string;
use function serialize;
use function unserialize;

/**
 * @internal
 */
final class EventQueue extends ThreadSafe{
	/** @var ThreadSafeArray<int, string> */
	private ThreadSafeArray $events;

	public function __construct(){
		$this->events = new ThreadSafeArray();
	}

	public function publish(EventType $type, array $payload) : void{
		$row = serialize([$type->value, $payload]);
		$this->synchronized(function() use ($row) : void{
			$this->events[] = $row;
			$this->notify();
		});
	}

	/**
	 * @return list<array{EventType, array<int|string, mixed>}>
	 */
	public function fetchAll() : array{
		/** @var list<string> $rows */
		$rows = $this->synchronized(function() : array{
			$rows = [];
			while(is_string($row = $this->events->shift())){
				$rows[] = $row;
			}
			return $rows;
		});
		$events = [];
		foreach($rows as $row){
			/** @var array{int, array<int|string, mixed>} $entry */
			$entry = unserialize($row, ["allowed_classes" => [\DateTime::class, \DateTimeImmutable::class]]);
			$events[] = [EventType::from($entry[0]), $entry[1]];
		}
		return $events;
	}

	public function waitForEvents(int $timeout) : void{
		$this->synchronized(function() use ($timeout) : void{
			if($this->events->count() === 0){
				$this->wait($timeout);
			}
		});
	}
}
