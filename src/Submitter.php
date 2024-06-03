<?php

declare(strict_types=1);

namespace G41797\Queue\Pulsar;

use Ramsey\Uuid\Uuid;

use Yiisoft\Queue\Message\IdEnvelope;
use Yiisoft\Queue\Message\JsonMessageSerializer;
use Yiisoft\Queue\Message\MessageInterface;

use Pulsar\Producer;
use Pulsar\ProducerOptions;
use Pulsar\MessageOptions;

class Submitter
{
    static public function default(): Submitter
    {
        return new Submitter
        (
            'pulsar://localhost:6650',
            Broker::channelToTopic(Adapter::DEFAULT_CHANNEL_NAME)
        );
    }

    private JsonMessageSerializer $serializer;

    private ?Producer $producer = null;

    public function __construct(
        private readonly string $url,
        private readonly string $topic
    ) {
        $this->serializer = new JsonMessageSerializer();
    }

    public function isConnected(): bool
    {
        if ($this->producer !== null) {
            return true;
        }

        return $this->connect();
    }

    private function connect(): bool
    {
        try {

            $options = new ProducerOptions();
            $options->setProducerName(Broker::PRODUCER_NAME);
            $options->setConnectTimeout(3);
            $options->setInitialSubscriptionName(Broker::SUBSCRIPTION_NAME);
            $options->setTopic($this->topic);

            $producer = new Producer($this->url, $options);
            $producer->connect();

            $this->producer = $producer;
            return true;
        }
        catch (\Throwable $exception) {
            return false;
        }
    }

    public function disconnect(): void
    {
        if ($this->producer !== null) {
            $this->producer->close();
            $this->producer = null;
        }
    }

    public function submit(MessageInterface $job): ?IdEnvelope
    {
        if (!$this->isConnected())
        {
            return null;
        }

        $envelope = null;

        try {
            // $uuid = Uuid::uuid7()->toString();
            $payload = $this->serializer->serialize($job);

            $mid = $this->producer->send
            (
                $payload,
                /*
                [
                    MessageOptions::PROPERTIES => ['jobid' => $uuid]
                ]
                */
            );

            $envelope = new IdEnvelope($job, $mid);
        }
        catch (\Throwable ) {
            $envelope = null; // For breakpoint
        } finally {
            $this->disconnect();
        }
        return $envelope;
    }


}
