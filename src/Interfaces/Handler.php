<?php

namespace PaulLebedev\Kafka\Interfaces;

use RdKafka\Message;
interface Handler
{
    public function handle(Message $message): void;
}