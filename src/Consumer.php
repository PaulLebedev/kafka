<?php

namespace PaulLebedev\Kafka;

use PaulLebedev\Kafka\Interfaces\Handler;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;

class Consumer {

    public function __construct(private Handler $handler, private string $topic) {}

    public function consume(): void {
        $conf = new Conf();

        $consumer = new KafkaConsumer($conf);
        
        // Subscribe to topic 
        $consumer->subscribe([$this->topic]);

        $is_completed = false;
        while (!$is_completed) {
            $message = $consumer->consume(120 * 1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->handler->handle($message);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "No more messages; exit" . PHP_EOL;
                    $is_completed = true;
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    throw new \Exception("Timed out");
                default:
                    throw new \Exception($message->errstr(), $message->err);
            }
        }
    }
}