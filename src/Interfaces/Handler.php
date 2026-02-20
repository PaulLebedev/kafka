<?php

namespace Src\Interfaces;

interface Handler {

    public function handle(Message $message): void;
}