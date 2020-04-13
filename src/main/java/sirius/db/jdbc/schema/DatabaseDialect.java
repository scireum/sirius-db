/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.schema;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;
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
     *
     * @param table the table object to enhance
     * @return the enhanced table object
     */
    Table completeTableInfos(Table table);

    /**
     * Converts the given class into the JDBC-Value (@see {@link Types}).
     *
     * @param clazz the type to convert
     * @return the appropiate type from <tt>Types</tt>
     */
    int getJDBCType(Class<?> clazz);

    /**
     * Determines if two given column are equal or not. This can gracefully
     * handle questions like is VARCHAR == CHAR etc.
     *
     * @param target  the target column as expected by the schema
     * @param current the current database column was read from the metadata
     * @return null if they are equal or a string which contains a reason why
     * they are not.
     */
    @Nullable
    String areColumnsEqual(TableColumn target, TableColumn current);

    /**
     * Builds a CREATE TABLE statement.
     *
     * @param table the table to create
     * @return the generated SQL statement or null if nothing should be done
     */
    @Nullable
    String generateCreateTable(Table table);

    /**
     * Builds a ALTER TABLE RENAME statement.
     *
     * @param table the table to create
     * @return the generated SQL statement or null if nothing should be done
     */
    @Nullable
    String generateRenameTable(Table table);

    /**
     * Builds a DROP TABLE statement.
     *
     * @param table the table to drop
     * @return the generated SQL statement or null if nothing should be done
     */
    @Nullable
    String generateDropTable(Table table);

    /**
     * Alters the given column.
     *
     * @param table    the table to alter
     * @param oldName  the old name of the column (if it was renamed)
     * @param toColumn the column as expected in the schema
     * @return the generated SQL statement or an empty list if nothing should be done
     */
    List<String> generateAlterColumnTo(Table table, @Nullable String oldName, TableColumn toColumn);

    /**
     * Generates an alter statement to add the given column.
     *
     * @param table the table to alter
     * @param col   the column to create
     * @return the generated SQL statement or null if nothing should be done
     */
    @Nullable
    String generateAddColumn(Table table, TableColumn col);

    /**
     * Generates an alter statement to drop the given column.
     *
     * @param table the table to alter
     * @param col   the column to drop
     * @return the generated SQL statement or null if nothing should be done
     */
    @Nullable
    String generateDropColumn(Table table, TableColumn col);

    /**
     * Alters the table so that the PK is updated.
     *
     * @param table the table to alter
     * @return the generated SQL statement or an empty list if nothing should be done
     */
    List<String> generateAlterPrimaryKey(Table table);

    /**
     * Alters the table so that the given key is updated
     *
     * @param table the table to alter
     * @param from  the key as currently present in the database
     * @param to    the key as defined in the schema
     * @return the generated SQL statement or an empty list if nothing should be done
     */
    List<String> generateAlterKey(Table table, Key from, Key to);

    /**
     * Alters the table so that the given key is added
     *
     * @param table the table to alter
     * @param key   the key to add
     * @return the generated SQL statement or null if nothing should be done
     */
    @Nullable
    String generateAddKey(Table table, Key key);

    /**
     * Alters the table so that the given key is dropped
     *
     * @param table the table to alter
     * @param key   the key to drop
     * @return the generated SQL statement or null if nothing should be done
     */
    @Nullable
    String generateDropKey(Table table, Key key);

    /**
     * Alters the table so that the given foreign key is updated
     *
     * @param table the table to alter
     * @param from  the key as currently present in the database
     * @param to    the key as defined in the schema
     * @return the generated SQL statements or an empty list if nothing should be done
     */
    List<String> generateAlterForeignKey(Table table, ForeignKey from, ForeignKey to);

    /**
     * Alters the table so that the given foreign key is added
     *
     * @param table the table to alter
     * @param key   the key to add
     * @return the generated SQL statement or null if nothing should be done
     */
    @Nullable
    String generateAddForeignKey(Table table, ForeignKey key);

    /**
     * Alters the table so that the given foreign key is dropped
     *
     * @param table the table to alter
     * @param key   the key to drop
     * @return the generated SQL statement or null if nothing should be done
     */
    @Nullable
    String generateDropForeignKey(Table table, ForeignKey key);

    /**
     * Converts the name (casing) to the one, used by the DB.
     *
     * @param name the name of the column
     * @return the name written as the DB expects it
     */
    String translateColumnName(String name);

    /**
     * Determines whether casing of columns should be fixed.
     *
     * @return <tt>true</tt> if column names are case sensitive
     */
    boolean isColumnCaseSensitive();

    /**
     * Determines if the given key should be dropped.
     *
     * @param targetTable  the table being referenced
     * @param currentTable the table referencing another
     * @param key          the key which defines the reference
     * @return <tt>true</tt> if the key should be dropped, <tt>false</tt> otherwise
     */
    boolean shouldDropKey(Table targetTable, Table currentTable, Key key);

    /**
     * Computes the effective name used for a key.
     *
     * @param targetTable the table which contains the key
     * @param key         the key itself
     * @return the effective name for the key
     */
    String getEffectiveKeyName(Table targetTable, Key key);

    /**
     * Determines the default value for the given column
     *
     * @param rs the result set of the column
     * @return the default value, can be <tt>null</tt>
     * @throws SQLException in case of a database error
     */
    String getDefaultValue(ResultSet rs) throws SQLException;
}
