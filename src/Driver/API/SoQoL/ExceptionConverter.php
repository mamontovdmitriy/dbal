<?php

declare(strict_types=1);

namespace Doctrine\DBAL\Driver\API\SoQoL;

use Doctrine\DBAL\Driver\API\ExceptionConverter as ExceptionConverterInterface;
use Doctrine\DBAL\Driver\Exception;
use Doctrine\DBAL\Exception\ConnectionException;
use Doctrine\DBAL\Exception\DatabaseObjectNotFoundException;
use Doctrine\DBAL\Exception\DriverException;
use Doctrine\DBAL\Exception\InvalidFieldNameException;
use Doctrine\DBAL\Exception\NonUniqueFieldNameException;
use Doctrine\DBAL\Exception\NotNullConstraintViolationException;
use Doctrine\DBAL\Exception\SyntaxErrorException;
use Doctrine\DBAL\Exception\TableExistsException;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Query;

use function strpos;

/** @internal */
final class ExceptionConverter implements ExceptionConverterInterface
{
    /**
     * {@inheritdoc}
     */
    public function convert(Exception $exception, ?Query $query): DriverException
    {
        switch ($exception->getCode()) {
            case -20014:
                return new UniqueConstraintViolationException($exception, $query);
            case -22509:
            case -23028:
            case 4294944787: // -22509
            case 4294944268: // -23028
                return new ConnectionException($exception, $query);
            case -24134:
                return new NotNullConstraintViolationException($exception, $query);
            case -25001:
                return new SyntaxErrorException($exception, $query);
            case -25014:
                return new NonUniqueFieldNameException($exception, $query);
            case -25016:
            case -25018:
                return new TableNotFoundException($exception, $query);
            case -25024:
                return new InvalidFieldNameException($exception, $query);
            case -25032:
                return new TableExistsException($exception, $query);
                /*
                return new DeadlockException($exception, $query);
                return new ForeignKeyConstraintViolationException($exception, $query);
                return new UniqueConstraintViolationException($exception, $query);
                return new DatabaseDoesNotExist($exception, $query);
                return new SchemaDoesNotExist($exception, $query);

                return new ConnectionException($exception, $query);
                */
        }

        return new DriverException($exception, $query);
    }
}
