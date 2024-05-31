<?php

declare(strict_types=1);

namespace G41797\Queue\Pulsar\Functional;

use G41797\Queue\Pulsar\Adapter;
use G41797\Queue\Pulsar\Broker;
use G41797\Queue\Pulsar\Cleaner;
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
        $this->assertEquals(10, $this->submitJobs(10));
        $this->assertEquals(10, (new Cleaner())->clean());
    }

    private function submitJobs(int $count): int
    {
        $submitted = 0;
        $submitter = Submitter::default();
        for ($i = 0; $i < $count; $i++) {
            $job = BrokerTest::defaultJob();
            $env = $submitter->submit($job);
            if (!$env) {
                break;
            }
            $submitted += 1;
        }
        return $submitted;
    }

}
