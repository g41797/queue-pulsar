# Yii3 Queue Adapter for Apache Pulsar


[![tests](https://github.com/g41797/queue-pulsar/actions/workflows/tests.yml/badge.svg)](https://github.com/g41797/queue-pulsar/actions/workflows/tests.yml)

## Description

Yii3 Queue Adapter for [**Apache Pulsar**](https://pulsar.apache.org/) is new adapter in [Yii3 Queue Adapters family.](https://github.com/yiisoft/queue/blob/master/docs/guide/en/adapter-list.md)

Implementation of adapter based on [pulsar-client-php](https://github.com/ikilobyte/pulsar-client-php) library.

## Requirements

- PHP 8.2 or higher.

## Installation

The package could be installed with composer:

```shell
composer require g41797/queue-pulsar
```

## General usage

- As part of [Yii3 Queue Framework](https://github.com/yiisoft/queue/blob/master/docs/guide/en/README.md)
- Stand-alone

## Limitations

[Job Status](https://github.com/yiisoft/queue/blob/master/docs/guide/en/usage.md#job-status)
```php
// Push a job into the queue and get a message ID.
$id = $queue->push(new SomeJob());

// Get job status.
$status = $queue->status($id);
```
is not supported.

## License

Yii3 Queue Adapter for Apache Pulsar is free software. It is released under the terms of the BSD License.
Please see [`LICENSE`](./LICENSE.md) for more information.
