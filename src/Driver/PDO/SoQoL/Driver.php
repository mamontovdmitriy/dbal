<?php

namespace Doctrine\DBAL\Driver\PDO\SoQoL;

use Doctrine\DBAL\Driver\AbstractSoQoLDriver;
use Doctrine\DBAL\Driver\PDO\Connection;
use Doctrine\DBAL\Driver\PDO\Exception;
use PDO;
use PDOException;

final class Driver extends AbstractSoQoLDriver
{
    /**
     * {@inheritdoc}
     *
     * @return Connection
     */
    public function connect(array $params): Connection
    {
        $driverOptions = $params['driverOptions'] ?? [];

        if (! empty($params['persistent'])) {
            $driverOptions[PDO::ATTR_PERSISTENT] = true;
        }

        try {
            $pdo = new PDO(
                $this->constructPdoDsn($params),
                $params['user'] ?? '',
                $params['password'] ?? '',
                $driverOptions,
            );
        } catch (PDOException $exception) {
            throw Exception::new($exception);
        }

        return new Connection($pdo);
    }

    /**
     * Constructs the PDO DSN.
     *
     * @param string[] $params
     */
    private function constructPdoDsn(array $params): string
    {
        $dsn = 'soqol:';

        if (isset($params['host']) && $params['host'] !== '') {
            $dsn .= 'host=' . $params['host'] . ';';
        }

        if (isset($params['port']) && $params['port'] !== '') {
            $dsn .= 'port=' . $params['port'] . ';';
        }

        if (isset($params['dbname']) && $params['dbname'] !== '') {
            $dsn .= 'dbname=' . $params['dbname'] . ';';
        }

        if (isset($params['charset']) && $params['charset'] !== '') {
            $dsn .= 'charset=' . $params['charset'] . ';';
        }

        return $dsn;
    }
}