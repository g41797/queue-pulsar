<?php

declare(strict_types=1);

namespace G41797\Queue\Pulsar\Functional;

use Pulsar\Producer;
use Pulsar\ProducerOptions;
use Pulsar\Consumer;
use Pulsar\ConsumerOptions;
use Pulsar\Exception\MessageNotFound;
use Pulsar\SubscriptionType;

class BaseTest extends FunctionalTestCase
{

    public function testProduceConsume(): void
    {
        $this->produceConsume(1000);
        $this->produceConsume(1);
    }

    private function produceConsume(int $receiveQueueSize)
    {
        $produced = self::produce();

        $this->assertGreaterThan(0, $produced);

        $consumed = self::consume($produced, $receiveQueueSize);

        $this->assertEquals($produced, $consumed);
    }

    static public function produce(): int
    {
        $count = 0;

        $options = new ProducerOptions();

        $options->setInitialSubscriptionName('workflows');
        $options->setConnectTimeout(3);
        $options->setTopic('persistent://public/default/demo');
        $producer = new Producer('pulsar://localhost:6650', $options);
        $producer->connect();

        for ($i = 0; $i < 10; $i++) {
            $messageID = $producer->send(sprintf('hello %d', $i));
            $count += 1;
        }

        // close
        $producer->close();

        return $count;
    }

    static public function consume(int $count, int $receiveQueueSize): int
    {
        $options = new ConsumerOptions();

        $options->setConnectTimeout(3);
        $options->setTopic('persistent://public/default/demo');
        $options->setSubscription('workflows');
        $options->setSubscriptionType(SubscriptionType::Shared);
        $options->setNackRedeliveryDelay(20);
        $options->setReceiveQueueSize($receiveQueueSize);
        $consumer = new Consumer('pulsar://localhost:6650', $options);
        $consumer->connect();

        $receive = $total = 0;

        while (true) {

            try {
                $message = $consumer->receive(false);
                $receive += 1;

                $consumer->ack($message);

                if ($receive == $count)
                {
                    break;
                }

            } catch (MessageNotFound $e) {
                if ($e->getCode() == MessageNotFound::Ignore) {
                    continue;
                }
                break;
            } catch (Throwable $e) {
                break;
            }
        }

        $consumer->close();

        return $receive;
    }
}
