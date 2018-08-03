/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.schema;

import sirius.kernel.di.std.Register;

import java.sql.Types;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;

/**
 * Defines the dialect used to sync the schema against a MySQL database.
 */
@Register(name = "mysql", classes = DatabaseDialect.class)
public class MySQLDatabaseDialect extends BasicDatabaseDialect {

    private static final String NOT_NULL = "NOT NULL";
    private static final String AUTO_INCREMENT = "AUTO_INCREMENT";

    @Override
    protected boolean areTypesEqual(int type, int other) {
        if (type == other) {
            return true;
        }
        if (in(type, other, Types.BOOLEAN, Types.BIT)) {
            return true;
        }
        if (in(type, other, Types.VARCHAR, Types.CHAR)) {
            return true;
        }
        if (in(type, other, Types.LONGVARCHAR, Types.CLOB)) {
            return true;
        }
        if (in(type, other, Types.LONGVARBINARY, Types.BLOB, Types.VARBINARY)) {
            return true;
        }

        return in(type, other, Types.NUMERIC, Types.DECIMAL);
    }

    @Override
    protected String getTypeName(TableColumn column) {
        if (Types.CHAR == column.getType()) {
            return "VARCHAR(" + ensurePositiveLength(column, 255) + ")";
        }

        return super.getTypeName(column);
    }

    @Override
    public String generateAddColumn(Table table, TableColumn col) {
        return MessageFormat.format("ALTER TABLE `{0}` ADD COLUMN `{1}` {2} {3} {4} {5}",
                                    table.getName(),
                                    col.getName(),
                                    getTypeName(col),
                                    col.isNullable() ? "" : NOT_NULL,
                                    col.isAutoIncrement() ? AUTO_INCREMENT : "",
                                    getDefaultValueAsString(col));
    }

    @Override
    public List<String> generateAlterColumnTo(Table table, String oldName, TableColumn toColumn) {
        String name = oldName;
        if (name == null) {
            name = toColumn.getName();
        }
        return Collections.singletonList(MessageFormat.format(
                "ALTER TABLE `{0}` CHANGE COLUMN `{1}` `{2}` {3} {4} {5} {6}",
                table.getName(),
                name,
                toColumn.getName(),
                getTypeName(toColumn),
                toColumn.isNullable() ? "" : NOT_NULL,
                toColumn.isAutoIncrement() ? AUTO_INCREMENT : "",
                getDefaultValueAsString(toColumn)));
    }

    @Override
    public String generateCreateTable(Table table) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE `");
        sb.append(table.getName());
        sb.append("` (\n");
        for (TableColumn col : table.getColumns()) {
            sb.append(MessageFormat.format("  `{0}` {1} {2} {3} {4},\n",
                                           col.getName(),
                                           getTypeName(col),
                                           col.isNullable() ? "" : NOT_NULL,
                                           col.isAutoIncrement() ? AUTO_INCREMENT : "",
                                           getDefaultValueAsString(col)));
        }
        for (Key key : table.getKeys()) {
            if (key.isUnique()) {
                sb.append(MessageFormat.format("   CONSTRAINT `{0}` UNIQUE ({1}),\n",
                                               key.getName(),
                                               String.join(", ", key.getColumns())));
            } else {
                sb.append(MessageFormat.format("   KEY `{0}` ({1}),\n",
                                               key.getName(),
                                               String.join(", ", key.getColumns())));
            }
        }
        // We rely on the sync tool, to generate the constraints in the next run. Otherwise table with cross-references
        // cannot be created. Therefore only the PK is generated....
        sb.append(MessageFormat.format(" PRIMARY KEY ({0})\n) ENGINE={1}",
                                       String.join(", ", table.getPrimaryKey()),
                                       getEngine(table).asString("InnoDB")));
        return sb.toString();
    }

    @Override
    public boolean isColumnCaseSensitive() {
        return true;
    }

    @Override
    public boolean shouldDropKey(Table targetTable, Table currentTable, Key key) {
        return true;
    }

    @Override
    protected int getConstraintCharacterLimit() {
        return 64;
    }
}
