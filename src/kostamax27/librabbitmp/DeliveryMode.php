<?php
/*
 * Copyright (C) 2026 kostamax27
 */

declare(strict_types=1);

namespace kostamax27\librabbitmp;

enum DeliveryMode: int{
	/**
	 * The message is kept in memory only and is lost if the broker restarts.
	 * This is the AMQP default when no delivery mode is specified.
	 */
	case TRANSIENT = 1;

	/**
	 * The message is written to disk and survives broker restarts - provided
	 * it sits in a *durable* queue; persistent messages in non-durable queues
	 * are still lost.
	 */
	case PERSISTENT = 2;
}
