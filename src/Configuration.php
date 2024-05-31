<?php

declare(strict_types=1);

namespace G41797\Queue\Pulsar;

final class Configuration
{
    public function __construct(
        public string   $host = 'localhost',
        public int      $port = 6650
    ) {
        return;
    }

    public function update(array $config): self
    {
        if (array_key_exists('host', $config))
        {
            $this->host = $config['host'];
        }

        if (array_key_exists('port', $config))
        {
            $this->port = $config['port'];
        }

        return $this;
    }

    public function url(): string
    {
        return 'pulsar://' . $this->host . ':' . $this->port;
    }

    static public function default(): self {
        return new self();
    }
}
