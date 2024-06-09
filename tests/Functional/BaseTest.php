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

    private function produceConsume(int $receiveQueueSize): void
    {
        $produced = self::produce();

        $this->assertGreaterThan(0, count($produced));

        $consumed = self::consume(count($produced), $receiveQueueSize);

        $this->assertEquals(count($produced), count($consumed));

        $this->assertEquals(0, count(array_diff($produced, $consumed)));
    }

    static public function produce(): array
    {
        $result = [];

        $options = new ProducerOptions();

        $options->setInitialSubscriptionName('workflows');
        $options->setConnectTimeout(3);
        $options->setTopic(self::defaultTopic());
        $producer = new Producer(self::defaultUrl(), $options);
        $producer->connect();

        for ($i = 0; $i < 100; $i++) {

            $job = self::defaultJob();
            $job->getMetadata()[$i] = $i;
            $payload = json_encode($job, JSON_THROW_ON_ERROR);

            $messageID = $producer->send($payload);

            $result[] = $payload;
        }

        // close
        $producer->close();

        return $result;
    }

    static public function consume(int $count, int $receiveQueueSize): array
    {
        $result = [];

        $options = new ConsumerOptions();

        $options->setConnectTimeout(3);
        $options->setTopic(self::defaultTopic());
        $options->setSubscription('workflows');
        $options->setSubscriptionType(SubscriptionType::Shared);
        $options->setNackRedeliveryDelay(20);
        $options->setReceiveQueueSize($receiveQueueSize);
        $consumer = new Consumer(self::defaultUrl(), $options);
        $consumer->connect();

        $receive = $total = 0;

        while (true) {

            try {
                $message = $consumer->receive(false);
                $result[] = $message->getPayload();
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

        return $result;
    }
}
