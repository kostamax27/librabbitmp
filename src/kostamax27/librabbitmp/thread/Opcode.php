<?php

declare(strict_types=1);

namespace kostamax27\librabbitmp\thread;

/**
 * @internal
 */
enum Opcode: int{
	case EXCHANGE_DECLARE = 1;
	case EXCHANGE_DELETE = 2;
	case QUEUE_DECLARE = 3;
	case QUEUE_BIND = 4;
	case QUEUE_UNBIND = 5;
	case QUEUE_DELETE = 6;
	case QUEUE_PURGE = 7;
	case PUBLISH = 8;
	case CONSUME = 9;
	case SUBSCRIBE_TOPIC = 10;
	case CANCEL = 11;
	case GET = 12;
	case ACK = 13;
	case NACK = 14;
	case QOS = 15;

	public function operationName() : string{
		return match($this){
			self::EXCHANGE_DECLARE => "exchangeDeclare",
			self::EXCHANGE_DELETE => "exchangeDelete",
			self::QUEUE_DECLARE => "queueDeclare",
			self::QUEUE_BIND => "queueBind",
			self::QUEUE_UNBIND => "queueUnbind",
			self::QUEUE_DELETE => "queueDelete",
			self::QUEUE_PURGE => "queuePurge",
			self::PUBLISH => "publish",
			self::CONSUME => "consume",
			self::SUBSCRIBE_TOPIC => "subscribe",
			self::CANCEL => "cancel",
			self::GET => "get",
			self::ACK => "ack",
			self::NACK => "nack",
			self::QOS => "qos"
		};
	}
}
