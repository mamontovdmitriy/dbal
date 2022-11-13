<?php

declare(strict_types=1);

namespace Doctrine\DBAL\Schema;

use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Platforms\Exception\NotSupported;
use Doctrine\DBAL\Platforms\SoQoLPlatform;
use Doctrine\DBAL\Result;
use Doctrine\DBAL\Types\Type;

use function array_change_key_case;
use function assert;
use function implode;
use function is_string;

use const CASE_LOWER;

/**
 * SoQoL Schema Manager.
 *
 * @extends AbstractSchemaManager<SoQoLPlatform>
 */
class SoQoLSchemaManager extends AbstractSchemaManager
{
    private ?string $currentSchema = null;

    /**
     * Drops a database.
     *
     * NOTE: You can not drop the database this SchemaManager is currently connected to.
     *
     * @throws Exception
     */
    public function dropDatabase(string $database): void
    {
        $this->connection->executeStatement('SHUTDOWN DATABASE ' . $database);
        $this->connection->executeStatement(
            $this->platform->getDropDatabaseSQL($database),
        );
    }

    /** @throws Exception */
    protected function selectTableNames(string $databaseName): Result
    {
        $sql = <<<'SQL'
SELECT 
    TABLE_NAME AS table_name
    , TABLE_SCHEM AS schema_name
FROM INFO.TABLES
WHERE 
    TABLE_TYPE = 'TABLE'
    AND TABLE_CAT = ?
    AND TABLE_SCHEM = ?
SQL;

        return $this->connection->executeQuery($sql, [$databaseName, $this->getCurrentSchema()]);
    }

    /**
     * Returns the name of the current schema.
     *
     * @throws Exception
     */
    protected function getCurrentSchema(): ?string
    {
        return $this->currentSchema ??= $this->determineCurrentSchema();
    }

    /**
     * Determines the name of the current schema.
     *
     * @throws Exception
     */
    protected function determineCurrentSchema(): string
    {
        $currentSchema = $this->connection->fetchOne('SELECT current_schema()');
        assert(is_string($currentSchema));

        return $currentSchema;
    }

    protected function selectTableColumns(string $databaseName, ?string $tableName = null): Result
    {
        $sql = <<<'SQL'
SELECT 
    C.TABLE_CAT
	, C.TABLE_SCHEM AS table_schema
	, C.TABLE_NAME AS table_name
	, C.COLUMN_NAME AS "field"
	, C.TYPE_NAME AS "type"
	, C.COLUMN_SIZE AS length
	, C.NULLABLE AS nullable
	, C.REMARKS AS "comment"
	, C.COLUMN_DEF AS "default"
FROM INFO.COLUMNS C
    INNER JOIN INFO.TABLES T ON C.TABLE_NAME = T.TABLE_NAME
WHERE 
    T.TABLE_TYPE = 'TABLE'
    AND %s
ORDER BY C.ORDINAL_POSITION
SQL;

        $conditions = ['T.TABLE_CAT = ?', 'C.TABLE_SCHEM = ?'];
        $params     = [$databaseName, $this->getCurrentSchema()];

        if ($tableName !== null) {
            $conditions[] = 'C.TABLE_NAME = ?';
            $params[]     = strtoupper($tableName); // UPPER TABLE_NAME
        }

        $sql = sprintf($sql, implode(' AND ', $conditions));

        return $this->connection->executeQuery($sql, $params);
    }

    protected function selectIndexColumns(string $databaseName, ?string $tableName = null): Result
    {
        $sql = <<<'SQL'
SELECT 
    TABLE_CAT
	, TABLE_SCHEM
	, TABLE_NAME
	, NON_UNIQUE
	, INDEX_QUALIFIER
	, INDEX_NAME
	, TYPE
	, ORDINAL_POSITION
	, COLUMN_NAME
	, ASC_OR_DESC
	, CARDINALITY
	, PAGES
	, FILTER_CONDITION
FROM INFO.TABLESTATISTICS
WHERE 
    %s 
ORDER BY ORDINAL_POSITION
SQL;

        $conditions = ['TABLE_CAT = ?', 'TABLE_SCHEM = ?'];
        $params     = [$databaseName, $this->getCurrentSchema()];

        if ($tableName !== null) {
            $conditions[] = 'TABLE_NAME = ?';
            $params[]     = strtoupper($tableName); // UPPER TABLE_NAME
        }

        $sql = sprintf($sql, implode(' AND ', $conditions));

        return $this->connection->executeQuery($sql, $params);
    }

    protected function selectForeignKeyColumns(string $databaseName, ?string $tableName = null): Result
    {
        return $this->connection->executeQuery('SELECT 1 WHERE 1=2'); // todo
    }

    /** @inheritdoc */
    protected function fetchTableOptionsByTable(string $databaseName, ?string $tableName = null): array
    {
        return []; // todo
    }

    /** @inheritdoc */
    protected function _getPortableTableColumnDefinition(array $tableColumn): Column
    {
        $tableColumn = array_change_key_case($tableColumn, CASE_LOWER);

        $matches = [];

        $autoincrement = false;

        if (
            $tableColumn['default'] !== null
            && preg_match("/^((\S+)\.)?(\S+)\.nextval$/i", $tableColumn['default'], $matches) === 1
        ) {
            $tableColumn['sequence'] = $matches[3]; // todo ?
            $tableColumn['default']  = null;
            $autoincrement           = true;
        }

        if (! isset($tableColumn['name'])) {
            $tableColumn['name'] = '';
        }

        $length    = null;
        $fixed     = false;
        $scale     = 0;
        $precision = null;

        $dbType = strtolower($tableColumn['type']);
        $dbType = strtok($dbType, '(), ');
        assert(is_string($dbType));

        $type = $this->platform->getDoctrineTypeMapping($dbType);

        switch ($dbType) {
            case 'char':
            case 'binary':
                $fixed = true;
                // no break
            case 'varchar':
                if (preg_match('/[a-z]+\s?\(([0-9]+)\)/', $tableColumn['type'], $matches) === 1) {
                    $length = (int) $matches[1];
                }
                break;

            case 'number':
            case 'decimal':
                if (preg_match('/[A-Za-z]+\(([0-9]+),\s?([0-9]+)\)/', $tableColumn['type'], $matches) === 1) {
                    $precision = (int) $matches[1];
                    $scale     = (int) $matches[2];
                } elseif (preg_match('/[A-Za-z]+\(([0-9]+)\)/', $tableColumn['type'], $matches) === 1) {
                    $precision = (int) $matches[1];
                }
                break;
        }

        $options = [
            'default'       => $tableColumn['default'],
            'notnull'       => ($tableColumn['nullable'] == 1), // $tableColumn['null'] !== 'YES',
            'length'        => $length,
            'fixed'         => $fixed,
            'scale'         => $scale,
            'precision'     => $precision,
            'unsigned'      => false,
            'autoincrement' => $autoincrement,
        ];

        // todo not yet
//        if (isset($tableColumn['comment'])) {
//            $options['comment'] = $tableColumn['comment'];
//        }

        $column = new Column($tableColumn['field'], Type::getType($type), $options);

        if (isset($tableColumn['characterset'])) {
            $column->setPlatformOption('charset', $tableColumn['characterset']);
        }

        if (isset($tableColumn['collation'])) {
            $column->setPlatformOption('collation', $tableColumn['collation']);
        }

        return $column;
    }

    /** @inheritdoc */
    protected function _getPortableTableDefinition(array $table): string
    {
        $table = array_change_key_case($table, CASE_LOWER);

        $currentSchema = $this->getCurrentSchema();

        if ($table['schema_name'] === $currentSchema) {
            return $table['table_name'];
        }

        return $table['schema_name'] . '.' . $table['table_name'];
    }

    /** @inheritdoc */
    protected function _getPortableViewDefinition(array $view): View
    {
        $view = array_change_key_case($view, CASE_LOWER);

        return new View($view['schema_name'] . '.' . $view['view_name'], $view['view_definition']);
    }

    /** @inheritdoc */
    protected function _getPortableTableForeignKeyDefinition(array $tableForeignKey): ForeignKeyConstraint
    {
        $tableForeignKey = array_change_key_case($tableForeignKey, CASE_LOWER);

        // TODO: Implement _getPortableTableForeignKeyDefinition() method.
        throw NotSupported::new(__METHOD__);
    }

    /**
     * @param array<string, mixed> $sequence
     *
     * @throws Exception
     */
    protected function _getPortableSequenceDefinition(array $sequence): Sequence
    {
        $sequence = array_change_key_case($sequence, CASE_LOWER);

        if ($sequence['schema_name'] !== $this->getCurrentSchema()) {
            $sequenceName = $sequence['schema_name'] . '.' . $sequence['sequence_name'];
        } else {
            $sequenceName = $sequence['sequence_name'];
        }

        return new Sequence($sequenceName, (int) $sequence['increment_by'], (int) $sequence['start_value']);
    }

    /** @inheritdoc  */
    protected function _getPortableTableIndexesList(array $tableIndexes, string $tableName): array
    {
        return []; // todo
    }
}
