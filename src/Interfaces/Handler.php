<?php

namespace Src\Interfaces;

interface Handler {

    public function handle($message): void;
}