<?php

declare(strict_types=1);

namespace G41797\Queue\Pulsar\Functional;

use Ramsey\Uuid\Uuid;
use Yiisoft\Queue\Enum\JobStatus;
use Yiisoft\Queue\Message\Message;
use G41797\Queue\Pulsar\Broker;
use Yiisoft\Queue\Message\MessageInterface;

class BrokerTest extends FunctionalTestCase
{
    static public function defaultJob(): MessageInterface
    {
        return new Message('jobhandler', 'jobdata');
    }
}
