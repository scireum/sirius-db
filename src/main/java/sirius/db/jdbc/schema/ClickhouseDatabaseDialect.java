/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.schema;

import sirius.kernel.commons.Monoflop;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Register;

import javax.annotation.Nullable;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;

/**
 * Defines the dialect used to sync the schema against a Clickhouse database.
 * <p>
 * Note that this is quite simple due to the fact that clickhouse cannot modify a schema after the fact
 * (one cannot add / drop columns once a table has been created).
 */
@Register(name = "clickhouse", classes = DatabaseDialect.class)
public class ClickhouseDatabaseDialect extends BasicDatabaseDialect {

    @Override
    public String generateCreateTable(Table table) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE `");
        sb.append(table.getName());
        sb.append("` (\n");
        Monoflop mf = Monoflop.create();
        for (TableColumn col : table.getColumns()) {
            if (mf.successiveCall()) {
                sb.append(",");
            }
            sb.append(MessageFormat.format("  {0} {1} {2}\n",
                                           col.getName(),
                                           getTypeName(col),
                                           getDefaultValueAsString(col)));
        }

        sb.append("\n) ENGINE=");
        sb.append(getEngine(table).asString("Log"));

        return sb.toString();
    }

    @Override
    protected String getTypeName(TableColumn column) {
        if (column.isNullable()) {
            return "Nullable(" + getClickHouseType(column) + ")";
        }
        return getClickHouseType(column);
    }

    private String getClickHouseType(TableColumn column) {
        int type = column.getType();
        if (type == Types.INTEGER) {
            return getIntType(column);
        }

        if (type == Types.BIGINT) {
            return "Int64";
        }

        if (type == Types.FLOAT) {
            return "Float32";
        }

        if (type == Types.DOUBLE) {
            return "Float64";
        }

        if (type == Types.DATE) {
            return "Date";
        }

        if (type == Types.TIMESTAMP) {
            return "DateTime";
        }

        if (type == Types.CHAR) {
            return getStringType(column);
        }

        if (type == Types.BOOLEAN) {
            return "Int8";
        }

        if (type == Types.ARRAY) {
            return "Array(String)";
        }

        throw new IllegalArgumentException(Strings.apply("The type %s (Property: %s) cannot be used in clickhouse!",
                                                         SchemaTool.getJdbcTypeName(type),
                                                         column.getSource()));
    }

    private String getStringType(TableColumn column) {
        if (column.getLength() == 0) {
            return "String";
        } else {
            return "FixedString(" + column.getLength() + ")";
        }
    }

    private String getIntType(TableColumn column) {
        if (column.getLength() == 1) {
            return "Int8";
        } else if (column.getLength() == 2) {
            return "Int16";
        } else if (column.getLength() == 4) {
            return "Int32";
        } else {
            throw new IllegalArgumentException(Strings.apply("Property: %s has an invalid length for an int type (%s)!",
                                                             column.getSource(),
                                                             column.getLength()));
        }
    }

    @Override
    public List<String> generateAlterColumnTo(Table table, @Nullable String oldName, TableColumn toColumn) {
        return Collections.emptyList();
    }

    @Override
    public String generateAddColumn(Table table, TableColumn col) {
        return null;
    }

    @Override
    public String generateAddForeignKey(Table table, ForeignKey key) {
        return null;
    }

    @Override
    public String generateAddKey(Table table, Key key) {
        return null;
    }

    @Override
    public List<String> generateAlterForeignKey(Table table, ForeignKey from, ForeignKey to) {
        return Collections.emptyList();
    }

    @Override
    public List<String> generateAlterKey(Table table, Key from, Key to) {
        return Collections.emptyList();
    }

    @Override
    public List<String> generateAlterPrimaryKey(Table table) {
        return Collections.emptyList();
    }

    @Override
    public String generateDropColumn(Table table, TableColumn col) {
        return null;
    }

    @Override
    public String generateDropForeignKey(Table table, ForeignKey key) {
        return null;
    }

    @Override
    public String generateDropKey(Table table, Key key) {
        return null;
    }

    @Override
    public boolean isColumnCaseSensitive() {
        return true;
    }

    @Override
    public boolean shouldDropKey(Table targetTable, Table currentTable, Key key) {
        return false;
    }

    @Override
    public String areColumnsEqual(TableColumn target, TableColumn current) {
        String reason = super.areColumnsEqual(target, current);

        if (Strings.isEmpty(reason)) {
            return null;
        }

        if (target.getType() == Types.BOOLEAN && current.getType() == Types.INTEGER && current.getLength() == 4) {
            // Clickhouse does not have a boolean type, so booleans are saved in "Int8" columns.
            // To make sure we have a "Int8" on our hands, we check for type == Integer and size == 4.
            // Clickhouse returns the size 4 for "Int8" columns, because e.g. "-127" has 4 digits including the sign...
            return null;
        }

        if (target.getType() == Types.CHAR
            && (current.getType() == Types.BLOB || current.getType() == Types.VARCHAR)
            && target.getLength() == current.getLength()) {
            // None of the Clickhouse String-Types translates back to the CHAR-Type.
            // "FixedString"s are type BLOB, "String"s are type VARCHAR.
            return null;
        }

        return reason;
    }
}
