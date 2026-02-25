<?php

namespace PaulLebedev\Kafka\Interfaces;

interface Handler {

    public function handle(Message $message): void;
}