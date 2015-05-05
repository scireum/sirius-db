package sirius.mixing.schema;

import java.sql.Types;
import java.util.List;

/**
 * Contains methods used by {@link SchemaTool} to generate SQL which conforms to
 * the dialect of a special database.
 */
public interface DatabaseDialect {

    /**
     * Performs DB-specific functions after a {@link Table} was read from the
     * DB-metadata. This can be used to change column types or default values.
     */
    Table completeTableInfos(Table table);

    /**
     * Converts the given class into the JDBC-Value (@see {@link Types}).
     */
    int getJDBCType(Class<?> clazz);

    /**
     * Determines if two given column are equal or not. This can gracefully
     * handle questions like is VARCHAR == CHAR etc.
     *
     * @return null if they are equal or a string which contains a reason why
     *         they are not.
     */
    String areColumnsEqual(TableColumn target, TableColumn current);

    /**
     * Builds a CREATE TABLE statement.
     */
    String generateCreateTable(Table table);

    /**
     * Builds a DROP TABLE statement.
     */
    String generateDropTable(Table table);

    /**
     * Alters the given column.
     */
    List<String> generateAlterColumnTo(Table table, String oldName, TableColumn toColumn);

    /**
     * Generates an alter statement to add the given column.
     */
    String generateAddColumn(Table table, TableColumn col);

    /**
     * Generates an alter statement to drop the given column.
     */
    String generateDropColumn(Table table, TableColumn col);

    /**
     * Alters the table so that the PK is updated.
     */
    List<String> generateAlterPrimaryKey(Table table);

    /**
     * Alters the table so that the given key is updated
     */
    List<String> generateAlterKey(Table table, Key from, Key to);

    /**
     * Alters the table so that the given key is added
     */
    String generateAddKey(Table table, Key key);

    /**
     * Alters the table so that the given key is dropped
     */
    String generateDropKey(Table table, Key key);

    /**
     * Alters the table so that the given foreign key is updated
     */
    List<String> generateAlterForeignKey(Table table, ForeignKey from, ForeignKey to);

    /**
     * Alters the table so that the given foreign key is added
     */
    String generateAddForeignKey(Table table, ForeignKey key);

    /**
     * Alters the table so that the given foreign key is dropped
     */
    String generateDropForeignKey(Table table, ForeignKey key);

    /**
     * Converts the name (casing) to the one, used by the DB.
     */
    String translateColumnName(String name);

    /**
     * Determines whether casing of columns should be fixed.
     */
    boolean isColumnCaseSensitive();

    /**
     * Determines if the given key should be dropped.
     */
    boolean shouldDropKey(Table targetTable, Table currentTable, Key key);
}
