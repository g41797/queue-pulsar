<?php

declare(strict_types=1);

namespace G41797\Queue\Pulsar;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

use Yiisoft\Queue\Adapter\AdapterInterface;
use Yiisoft\Queue\Cli\LoopInterface;
use Yiisoft\Queue\Enum\JobStatus;
use Yiisoft\Queue\Message\MessageInterface;
use Yiisoft\Queue\QueueFactoryInterface;

use G41797\Queue\Pulsar\Exception\NotSupportedStatusMethodException;

class Adapter implements AdapterInterface
{
    public const DEFAULT_CHANNEL_NAME = 'public/default/yii-queue';

    private BrokerFactoryInterface $brokerFactory;

    public function __construct(
        private string              $channelName = self::DEFAULT_CHANNEL_NAME,
        private array               $brokerConfiguration = [],
        private ?LoggerInterface    $logger = null,
        private ?LoopInterface      $loop = null,
        private float               $timeoutSec = 2.0,
    ) {
        $this->brokerFactory = new BrokerFactory();

        if (null == $loop ) {
            $loop = new NullLoop();
        }

        if (null == $logger) {
            $this->logger = new NullLogger();
        }

        $this->getBroker();
    }

    public function push(MessageInterface $message): MessageInterface
    {
        return $this->broker->push($message);
    }

    public function status(int|string $id): JobStatus
    {
        throw new NotSupportedStatusMethodException();
    }

    public function runExisting(callable $handlerCallback): void
    {
        $this->processJobs($handlerCallback, continueOnEmptyQueue: false);
    }

    public function subscribe(callable $handlerCallback): void
    {
        $this->processJobs($handlerCallback, continueOnEmptyQueue: true);
    }

    private function processJobs(callable $handlerCallback, bool $continueOnEmptyQueue): void
    {
        $result = true;
        while ($this->loop->canContinue() && $result === true) {
            $job = $this->broker->pull($this->timeoutSec);
            if (null === $job) {
                if ($continueOnEmptyQueue)
                {
                    continue;
                }
                break;
            }
            $result = $handlerCallback($job);
            $this->broker->done($job->getId());
        }
    }

    public function withChannel(string $channel): AdapterInterface
    {
        if ($channel == $this->channelName) {
            return $this;
        }

        return new self
        (
            channelName:            $this->channelName,
            brokerConfiguration:    $this->brokerConfiguration,
            logger:                 $this->logger,
            loop:                   $this->loop,
            timeoutSec:             $this->timeoutSec
        );
    }

    private ?BrokerInterface $broker = null;

    private function getBroker(): ?BrokerInterface
    {
        if ($this->broker == null) {
            $this->broker = $this->brokerFactory->get
                                    (
                                        $this->channelName,
                                        $this->brokerConfiguration,
                                        $this->logger
                                    );
        }
        return $this->broker;
    }
}
