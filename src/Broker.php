<?php

declare(strict_types=1);

namespace G41797\Queue\Pulsar;

use G41797\Queue\Pulsar\Exception\NotSupportedStatusMethodException;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

use Yiisoft\Queue\Enum\JobStatus;
use Yiisoft\Queue\Message\IdEnvelope;
use Yiisoft\Queue\Message\MessageInterface;

use G41797\Queue\Pulsar\Configuration as BrokerConfiguration;


class Broker implements BrokerInterface
{
    public const SUBSCRIPTION_NAME = 'jobs';

    public const CONSUMER_NAME = 'worker';

    public const PRODUCER_NAME = 'submitter';

    public array $statusString =
        [
            JobStatus::WAITING => 'WAITING',
            JobStatus::RESERVED => 'RESERVED',
            JobStatus::DONE => 'DONE'
        ];

    private string $topic;
    private string $url;

    public function __construct(
        string                          $channelName = Adapter::DEFAULT_CHANNEL_NAME,
        private ?BrokerConfiguration    $configuration = null,
        private ?LoggerInterface        $logger = null
    ) {
        if (empty($channelName)) {
            $channelName = Adapter::DEFAULT_CHANNEL_NAME;
        }

        $this->topic = self::channelToTopic($channelName);

        if (null == $configuration) {
            $this->configuration = BrokerConfiguration::default();
        }

        $this->url = $this->configuration->url();

        if (null == $logger) {
            $this->logger = new NullLogger();
        }
    }


    public function withChannel(string $channel): BrokerInterface
    {
        if ($channel == $this->channelName) {
            return $this;
        }

        return new self($channel, $this->configuration, $this->logger);
    }

    private ?Submitter $submitter = null;

    public function push(MessageInterface $job): ?IdEnvelope
    {
        if ($this->submitter == null)
        {
            $this->submitter = new Submitter($this->url, $this->topic);
        }

        $env = $this->submitter->submit($job);

        if ($env == null)
        {
            $this->submitter->disconnect();
            $this->submitter = null;
        }

        return $env;
    }

    public function jobStatus(string $id): ?JobStatus
    {
        throw new NotSupportedStatusMethodException();
    }


    private ?Receiver $receiver = null;

    public function pull(float $timeout): ?IdEnvelope
    {
        if ($this->receiver == null)
        {
            $this->receiver = new Receiver($this->url, $this->topic);
        }

        try {
            $msg = $this->receiver->receive($timeout);
            return $msg;
        }
        catch (\Exception $exc) {
            $this->receiver->disconnect();
            $this->receiver = null;
            return null;
        }
    }

    public function done(string $id): bool
    {
        // For automatic ACK after consuming.
        return !empty($id);
    }

    public function disconnect(): void
    {
    }

    static public function stringToJobStatus(string $status): ?JobStatus
    {
        return match ($status) {
            'WAITING' => JobStatus::waiting(),
            'RESERVED' => JobStatus::reserved(),
            'DONE' => JobStatus::done(),
            default => null,
        };
    }

    static public function channelToTopic(string $channel): string
    {
       return 'persistent://'.$channel;
    }
}
