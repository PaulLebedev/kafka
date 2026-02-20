<?php

namespace Src;

use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use Src\Interfaces\Handler;

class Consumer {

    public function __construct(private Handler $handler, private string $topic) {}

    public function consume(): void {
        $conf = new Conf();

        // Set the group id. This is required when storing offsets on the broker
        // Configure the group.id. All consumer with the same group.id will consume
        // different partitions.
        $conf->set('group.id', env('KAFKA_CONSUMER_GROUP_ID'));
        $conf->set('sasl.mechanisms', env('KAFKA_MECHANISMS'));
        $conf->set('sasl.username', env('KAFKA_USERNAME'));
        $conf->set('sasl.password', env('KAFKA_PASSWORD'));
        $conf->set('security.protocol', env('KAFKA_SECURITY_PROTOCOL'));
        $conf->set('ssl.ca.location', env('KAFKA_SSL_CA_LOCATION'));

        // Initial list of Kafka brokers
        $conf->set('metadata.broker.list', env('KAFKA_BROKERS'));

        // Set where to start consuming messages when there is no initial offset in
        // offset store or the desired offset is out of range.
        // 'earliest': start from the beginning
        $conf->set('auto.offset.reset', 'earliest');

        // Emit EOF event when reaching the end of a partition
        $conf->set('enable.partition.eof', 'true');

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