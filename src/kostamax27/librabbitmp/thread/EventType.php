<?php
/*
 * Copyright (C) 2026 kostamax27
 */

declare(strict_types=1);

namespace kostamax27\librabbitmp\thread;

/**
 * @internal
 */
enum EventType: int{
	/**
	 * A command completed successfully. Payload: command id, result data.
	 */
	case RESPONSE_OK = 1;

	/**
	 * A command failed. Payload: command id, message, code, connection-level flag.
	 */
	case RESPONSE_ERROR = 2;

	/**
	 * A message was pushed to a consumer. Payload: encoded message fields.
	 */
	case DELIVERY = 3;

	/**
	 * The connection to the broker dropped. Payload: reason, permanent flag.
	 */
	case CONNECTION_LOST = 4;

	/**
	 * The worker re-established the connection and restored consumers / QoS.
	 */
	case CONNECTION_RESTORED = 5;

	/**
	 * A consumer could not be restored after a channel / connection recovery,
	 * most commonly because its (auto-delete or exclusive) queue no longer
	 * exists. Payload: consumer tag, reason.
	 */
	case CONSUMER_LOST = 6;
}
