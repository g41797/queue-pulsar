<?php

namespace G41797\Queue\Pulsar;

use G41797\Queue\Pulsar\Exception\NotConnectedPulsarException;
use Pulsar\Consumer;
use Pulsar\ConsumerOptions;
use Pulsar\Exception\MessageNotFound;
use Pulsar\SubscriptionType;
use Yiisoft\Queue\Message\IdEnvelope;
use Yiisoft\Queue\Message\JsonMessageSerializer;

class Receiver
{
    protected ?Consumer   $consumer = null;
    private JsonMessageSerializer $serializer;

    public function __construct(
        private readonly string $url,
        private readonly string $topic,
        private readonly int    $receiveQueueSize   = 1,
    ) {
        $this->serializer = new JsonMessageSerializer();
    }

    public function receive(float $timeoutSec = 2.0): ?IdEnvelope
    {
        if (!$this->isConnected()) {
            throw new NotConnectedPulsarException();
        }

        $finish = microtime(true) + $timeoutSec;

        while (true)
        {
            try
            {
                $message = $this->consumer->receive(false);
                $this->consumer->ack($message);

                $job        = $this->serializer->unserialize($message->getPayload());
                // $uuid       = $message->getProperties()['jobid'] ?? "";
                $mid = $message->getMessageId();
                $envelope   = new IdEnvelope($job, $mid);

                return $envelope;
            }
            catch (MessageNotFound $e) {
                if (microtime(true) <= $finish)
                {
                    continue;
                }
                break;
            }
            catch (\Exception $e)
            {
                throw $e;
            }

        }

        return null;
    }

    public function clean(): int
    {
        $cleaned = -1;

        if (!$this->isConnected())
        {
            return $cleaned;
        }

        $cleaned = 0;

        while (true) {

            try {
                $env = $this->receive(2.0);
                if ($env == null)
                {
                    break;
                }
                $cleaned += 1;
            }
            catch (\Exception  $exc) {
                break;
            }
        }

        $this->consumer->close();
        $this->consumer = null;

        return $cleaned;
    }

    public function isConnected(): bool
    {
        if ($this->consumer !== null) {
            return true;
        }

        return $this->connect();
    }

    private function connect(): bool
    {
        try {

            $options = new ConsumerOptions();
            $options->setConsumerName(Broker::CONSUMER_NAME);
            $options->setConnectTimeout(3);
            $options->setTopic($this->topic);
            $options->setSubscription(Broker::SUBSCRIPTION_NAME);
            $options->setSubscriptionType(SubscriptionType::Shared);
            $options->setReconnectPolicy(true);
            $options->setReceiveQueueSize($this->receiveQueueSize);
            $options->setNackRedeliveryDelay(3);

            $consumer = new Consumer($this->url, $options);
            $consumer->connect();

            $this->consumer = $consumer;
            return true;
        }
        catch (\Throwable $exception) {
            return false;
        }
    }

    public function disconnect(): void
    {
        if ($this->consumer !== null)
        {
            $this->consumer->close();
            $this->consumer = null;
        }
    }
}
