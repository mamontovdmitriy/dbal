<?php

declare(strict_types=1);

namespace Doctrine\DBAL\Platforms;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\Exception\NotSupported;
use Doctrine\DBAL\Platforms\Keywords\KeywordList;
use Doctrine\DBAL\Platforms\Keywords\SoQoLKeywords;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Sequence;
use Doctrine\DBAL\Schema\SoQoLSchemaManager;
use Doctrine\DBAL\Schema\TableDiff;
use Doctrine\DBAL\TransactionIsolationLevel;

use function implode;
use function in_array;
use function sprintf;

/**
 * Provides the behavior, features and SQL dialect of the SoQoL database platform of the oldest supported version.
 */
class SoQoLPlatform extends AbstractPlatform
{
    public function createSchemaManager(Connection $connection): AbstractSchemaManager
    {
        return new SoQoLSchemaManager($connection, $this);
    }

    /**
     * Returns the SQL to create a new database.
     *
     * @param string $name The name of the database that should be created.
     */
    public function getCreateDatabaseSQL(string $name): string
    {
        return ' create database "' . $name . '" on \'./' . $name . '\'';
    }

    public function getLocateExpression(string $string, string $substring, ?string $start = null): string
    {
        if ($start === null) {
            return 'INSTR(' . $substring . ', ' . $string . ')';
        }

        return 'INSTR(' . $substring . ', ' . $string . ', ' . $start . ')';
    }

    public function getDateDiffExpression(string $date1, string $date2): string
    {
//        return sprintf('TIMESTAMP_DIFF(DAY, %s, %s)', $date1, $date2);
        return sprintf('timestamp_diff(day, timestamp_trunc(day, %s), timestamp_trunc(day, %s)) + 1', $date1, $date2);
    }

    /**
     * {@inheritDoc}
     */
    public function getAlterTableSQL(TableDiff $diff): array
    {
        return []; // todo
    }

    public function getListViewsSQL(string $database): string
    {
        //  $view['schema_name'] . '.' . $view['view_name'], $view['view_definition']
        return <<<'SQL'
SELECT
    S.NAME AS schema_name
    , T.NAME AS view_name
    , V.TEXT AS view_definition
FROM SYS._OBJECT T
INNER JOIN SYS._VIEW V ON V.VIEW_ID = T.OBJ_ID
INNER JOIN SYS._SCHEMA S ON S.SCHEMA_ID = T.SCHEMA_ID
WHERE T.TYPE = 5
ORDER BY S.NAME ASC, T.NAME ASC
SQL;
    }

    public function getSetTransactionIsolationSQL(TransactionIsolationLevel $level): string
    {
        return 'SET TRANSACTION ISOLATION LEVEL ' . $this->_getTransactionIsolationLevelSQL($level);
    }

    /**
     * Returns the SQL for a given transaction isolation level Connection constant.
     */
    protected function _getTransactionIsolationLevelSQL(TransactionIsolationLevel $level): string
    {
        // todo SNAPSHOT
        return match ($level) {
//            TransactionIsolationLevel::READ_UNCOMMITTED => 'READ UNCOMMITTED',
            TransactionIsolationLevel::READ_COMMITTED => 'READ COMMITTED',
//            TransactionIsolationLevel::REPEATABLE_READ => 'REPEATABLE READ',
            TransactionIsolationLevel::SERIALIZABLE => 'SERIALIZABLE',
            default => throw NotSupported::new(__METHOD__),
        };
    }

    /** @inheritdoc */
    public function getDecimalTypeDeclarationSQL(array $column): string
    {
        $arguments = '';
        if (isset($column['precision'])) {
            $precision = $column['precision'];
            $scale     = $column['scale'] ?? null;
            $arguments = $scale
                ? '(' . $precision . ', ' . $scale . ')'
                : '(' . $precision . ')';
        }

        return 'DECIMAL ' . $arguments;
    }

    /** @inheritdoc */
    public function getDateTimeTypeDeclarationSQL(array $column): string
    {
        return 'TIMESTAMP';
    }

    /** @inheritdoc */
    public function getDateTypeDeclarationSQL(array $column): string
    {
        return 'DATE';
    }

    /** @inheritdoc */
    public function getTimeTypeDeclarationSQL(array $column): string
    {
        return 'TIMESTAMP';
    }

    /** @inheritdoc */
    public function getBooleanTypeDeclarationSQL(array $column): string
    {
        return 'BOOLEAN';
    }

    /** @inheritdoc */
    public function getIntegerTypeDeclarationSQL(array $column): string
    {
        return 'INTEGER';
    }

    /** @inheritdoc */
    public function getBigIntTypeDeclarationSQL(array $column): string
    {
        return 'BIGINT';
    }

    /** @inheritdoc */
    public function getSmallIntTypeDeclarationSQL(array $column): string
    {
        return 'SMALLINT';
    }

    /** @inheritdoc */
    public function getClobTypeDeclarationSQL(array $column): string
    {
        return 'CLOB';
    }

    /** @inheritdoc */
    public function getBlobTypeDeclarationSQL(array $column): string
    {
        return 'BLOB';
    }

    public function getCurrentDatabaseExpression(): string
    {
        return 'CURRENT_DBNAME()';
    }

    public function supportsSequences(): bool
    {
        return true;
    }

    public function supportsSchemas(): bool
    {
        return true;
    }

    public function supportsIdentityColumns(): bool
    {
        return false;
    }

    /**
     * @internal The method should be only used from within the {@see AbstractSchemaManager} class hierarchy.
     *
     * @inheritdoc
     */
    public function getListSequencesSQL(string $database): string
    {
        $sql = <<<'SQL'
SELECT 
    SEQUENCE_NAME AS sequence_name
    , SEQUENCE_SCHEMA AS schema_name
    , START_VALUE AS start_value
    , INCREMENT_VALUE AS increment_by
FROM INFO.SEQUENCES
WHERE SEQUENCE_CATALOG = %s
    AND SEQUENCE_SCHEMA != 'PUBLIC'
    AND SEQUENCE_SCHEMA != 'SYS'
    AND SEQUENCE_SCHEMA != 'INFO'
SQL;

        return sprintf($sql, $this->quoteStringLiteral($database));
    }

    public function getAlterSequenceSQL(Sequence $sequence): string
    {
        return 'ALTER SEQUENCE ' . $sequence->getQuotedName($this) .
            ' INCREMENT BY ' . $sequence->getAllocationSize() .
            $this->getSequenceCacheSQL($sequence);
    }

    /**
     * Cache definition for sequences
     */
    private function getSequenceCacheSQL(Sequence $sequence): string
    {
        if ($sequence->getCache() > 1) {
            return ' CACHE ' . $sequence->getCache();
        }

        return '';
    }

    public function getCreateSequenceSQL(Sequence $sequence): string
    {
        return 'CREATE SEQUENCE ' . $sequence->getQuotedName($this) .
            ' INCREMENT BY ' . $sequence->getAllocationSize() .
            ' MINVALUE ' . $sequence->getInitialValue() .
            ' START WITH ' . $sequence->getInitialValue() .
            $this->getSequenceCacheSQL($sequence);
    }

    public function getDropSequenceSQL(string $name): string
    {
        return parent::getDropSequenceSQL($name) . ' CASCADE';
    }

    public function getSequenceNextValSQL(string $sequence): string
    {
        return 'SELECT ' . $sequence . '.NEXTVAL';
    }

    /**
     * Returns the regular expression operator.
     */
    public function getRegexpExpression(): string
    {
        throw NotSupported::new(__METHOD__); // todo

//        return 'REGEXP_LIKE';
    }

    /**
     * Returns a SQL snippet to concatenate the given strings.
     */
    public function getConcatExpression(string ...$string): string
    {
        return 'TRIM(' . implode(' || ', $string) . ')';
    }

    /**
     * Returns the SQL snippet to trim a string.
     *
     * @param string      $str  The expression to apply the trim to.
     * @param TrimMode    $mode The position of the trim.
     * @param string|null $char The char to trim, has to be quoted already. Defaults to space.
     */
    public function getTrimExpression(
        string $str,
        TrimMode $mode = TrimMode::UNSPECIFIED,
        ?string $char = null,
    ): string {
        if (in_array($mode, [TrimMode::UNSPECIFIED, TrimMode::BOTH], true)) {
            if ($char === null) {
                return sprintf('LTRIM(RTRIM(%s))', $str);
            }

            return sprintf('LTRIM(RTRIM(%s, %s), %s)', $str, $char, $char);
        }

        $trimFn = match ($mode) {
            TrimMode::LEADING => 'LTRIM',
            TrimMode::TRAILING => 'RTRIM',
        };

        if ($char === null) {
            return sprintf('%s(%s)', $trimFn, $str);
        }

        return sprintf('%s(%s, %s)', $trimFn, $str, $char);
    }

    public function getSubstringExpression(string $string, string $start, ?string $length = null): string
    {
        if ($length === null) {
            return sprintf('SUBSTR(%s, %s)', $string, $start);
        }

        return sprintf('SUBSTR(%s, %s, %s)', $string, $start, $length);
    }

    protected function createReservedKeywordsList(): KeywordList
    {
        return new SoQoLKeywords();
    }

    /** @inheritdoc */
    protected function _getCommonIntegerTypeDeclarationSQL(array $column): string
    {
        return '';
    }

//    public function getColumnDeclarationListSQL(array $columns): string
//    {
//        return parent::getColumnDeclarationListSQL($columns); // TODO: Change the autogenerated stub
//    }

    protected function _getCreateTableSQL(string $name, array $columns, array $options = []): array
    {
        $sql = [];

        foreach ($columns as &$column) {
            if ($column["autoincrement"] === true && $column["primary"] === true) {
                $sequenceName = sprintf('%s_%s_seq', $name, $column["name"]);

                $column["default"] = $sequenceName . '.nextval';
                $column["notnull"] = false;

                $sql[] = $this->getCreateSequenceSQL(new Sequence($sequenceName));
            }
        }

        return array_merge($sql, parent::_getCreateTableSQL($name, $columns, $options));
    }

    protected function getDateArithmeticIntervalExpression(
        string $date,
        string $operator,
        string $interval,
        DateIntervalUnit $unit,
    ): string {
        $sign = $operator === '+' ? 1 : -1;

        return 'TIMESTAMP_ADD(' . $unit->value . ',  ' . $sign . '*' . $interval . ', ' . $date . ' )';
    }

    protected function initializeDoctrineTypeMappings(): void
    {
        $this->doctrineTypeMapping = [
            // soqol    => doctrine
            'bigint'    => 'bigint',
            'binary'    => 'blob',
            'blob'      => 'blob',
            'boolean'   => 'boolean',
            'char'      => 'string',
            'clob'      => 'text',
            'date'      => 'date',
            'decimal'   => 'decimal',
//            'float'     => 'float',
            'int'       => 'integer',
            'integer'   => 'integer',
            'number'    => 'decimal',
//            'real'      => 'float',
            'rowid'     => 'string',
            'smallint'  => 'smallint',
            'timestamp' => 'datetime',
            'varbinary' => 'blob',
            'varchar'   => 'string',
        ];
    }
}
