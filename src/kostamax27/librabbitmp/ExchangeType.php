<?php
/*
 * Copyright (C) 2026 kostamax27
 */

declare(strict_types=1);

namespace kostamax27\librabbitmp;

enum ExchangeType: string{
	/**
	 * Routes to queues whose binding key exactly matches the routing key.
	 */
	case DIRECT = "direct";

	/**
	 * Routes to every bound queue, ignoring the routing key.
	 */
	case FANOUT = "fanout";

	/**
	 * Routes using dot-separated wildcard patterns ("*" = one word, "#" = zero or more words).
	 */
	case TOPIC = "topic";

	/**
	 * Routes by matching message headers against binding arguments.
	 */
	case HEADERS = "headers";
}
