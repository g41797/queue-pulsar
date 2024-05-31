<?php

declare(strict_types=1);

namespace G41797\Queue\Pulsar;

use Pulsar\Consumer;
use Pulsar\ConsumerOptions;
use Pulsar\Exception\MessageNotFound;
use Pulsar\SubscriptionType;


class Cleaner
{
    private ?Consumer   $consumer = null;

    public function __construct(
        private readonly string $url            = 'pulsar://localhost:6650',
        private readonly string $channelName    = Adapter::DEFAULT_CHANNEL_NAME
    ) {
    }

    public function clean(): bool
    {
        if (!$this->isConnected())
        {
            return false;
        }

        while (true) {

            try {
                $message = $this->consumer->receive(false);
                $this->consumer->ack($message);
            }
            catch (MessageNotFound $e) {
                    break;
            }
        }

        $this->consumer->close();
        $this->consumer = null;

        return true;
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
            $options->setTopic(Broker::channelToTopic($this->channelName));
            $options->setSubscription(Broker::SUBSCRIPTION_NAME);
            $options->setSubscriptionType(SubscriptionType::Shared);
            $options->setNackRedeliveryDelay(1);

            $consumer = new Consumer($this->url, $options);
            $consumer->connect();

            $this->consumer = $consumer;
            return true;
        }
        catch (\Throwable $exception) {
            return false;
        }
    }

}
