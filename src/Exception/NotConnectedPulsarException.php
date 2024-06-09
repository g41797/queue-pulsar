<?php

declare(strict_types=1);

namespace G41797\Queue\Pulsar\Exception;

use Yiisoft\FriendlyException\FriendlyExceptionInterface;

class NotConnectedPulsarException extends \RuntimeException implements FriendlyExceptionInterface
{
    public function getName(): string
    {
        return 'Not connected to Pulsar.';
    }

    public function getSolution(): ?string
    {
        return 'Check your Pulsar configuration.';
    }
}

