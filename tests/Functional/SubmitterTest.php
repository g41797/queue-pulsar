<?php

declare(strict_types=1);

namespace G41797\Queue\Pulsar\Functional;

use G41797\Queue\Pulsar\Adapter;
use G41797\Queue\Pulsar\Broker;
use G41797\Queue\Pulsar\Receiver;
use G41797\Queue\Pulsar\Submitter;

class SubmitterTest extends FunctionalTestCase
{
    public function testSetUp(): void
    {
        $this->assertTrue(true);
        return;
    }

    public function testSubmit(): void
    {
        $count = 10;
        $this->assertEquals($count, count($this->submitJobs($count)));
        $this->assertEquals($count, (new Receiver(receiveQueueSize: 1000))->clean());
    }

    private function submitJobs(int $count): array
    {
        $submitted = [];

        for ($i = 0; $i < $count; $i++) {
            $submitter = Submitter::default();
            $job = BrokerTest::defaultJob();
            $env = $submitter->submit($job);
            if ($env == null) {
                break;
            }
            $submitted[] = $env;
        }
        return $submitted;
    }

}
