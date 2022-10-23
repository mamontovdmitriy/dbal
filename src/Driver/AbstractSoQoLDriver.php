<?php

namespace Doctrine\DBAL\Driver;

use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\API\ExceptionConverter;
use Doctrine\DBAL\Driver\API\SoQoL;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\SoQoLPlatform;
use Doctrine\DBAL\ServerVersionProvider;

/**
 * Abstract base implementation of the {@see Driver} interface for SoQoL based drivers.
 */
abstract class AbstractSoQoLDriver implements Driver
{
    /**
     * {@inheritdoc}
     */
    public function getDatabasePlatform(ServerVersionProvider $versionProvider): AbstractPlatform
    {
        return new SoQoLPlatform();
    }

    /**
     * @return ExceptionConverter
     */
    public function getExceptionConverter(): ExceptionConverter
    {
        return new SoQoL\ExceptionConverter();
    }
}
