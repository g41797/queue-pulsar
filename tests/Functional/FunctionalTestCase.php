<?php

declare(strict_types=1);

namespace G41797\Queue\Pulsar\Functional;

use G41797\Queue\Pulsar\Adapter;
use G41797\Queue\Pulsar\Broker;
use G41797\Queue\Pulsar\Configuration;
use G41797\Queue\Pulsar\Receiver;
use PHPUnit\Framework\TestCase;
use RdKafka\Conf;
use Yiisoft\Queue\Message\Message;
use Yiisoft\Queue\Message\MessageInterface;


abstract class FunctionalTestCase extends TestCase
{
    public function setUp(): void
    {
        $this->clean();

        parent::setUp();
    }
    public function tearDown(): void
    {
        $this->clean();

        parent::tearDown();
    }
    public function clean(): void
    {
        $url = self::defaultUrl();
        $topic = self::defaultTopic();

        $this->assertGreaterThanOrEqual(0, (new Receiver($url, $topic, receiveQueueSize: 10000))->clean());
    }
    static public function defaultJob(): MessageInterface
    {
        return new Message('jobhandler', 'jobdata', metadata: []);
    }

    static public function defaultUrl(): string
    {
        return Configuration::default()->url();
    }

    static public function defaultTopic(): string
    {
        return Broker::channelToTopic(Adapter::DEFAULT_CHANNEL_NAME);
    }
}
