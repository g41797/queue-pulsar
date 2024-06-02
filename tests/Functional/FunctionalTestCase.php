<?php

declare(strict_types=1);

namespace G41797\Queue\Pulsar\Functional;

use G41797\Queue\Pulsar\Receiver;
use PHPUnit\Framework\TestCase;


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
        $this->assertGreaterThanOrEqual(0, (new Receiver(receiveQueueSize: 10000))->clean());
    }
}
