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
            if (!col.isAutoIncrement()) {
                if (mf.successiveCall()) {
                    sb.append(",");
                }
                sb.append(MessageFormat.format("  {0} {1} {2}\n",
                                               col.getName(),
                                               getTypeName(col),
                                               getDefaultValueAsString(col)));
            }
        }

        sb.append("\n) ENGINE=");
        sb.append(getEngine(table).asString("Log"));

        return sb.toString();
    }

    @Override
    protected String getTypeName(TableColumn column) {
        int type = column.getType();
        if (type == Types.INTEGER) {
            if (column.getLength() == 1) {
                return "Int8";
            } else if (column.getLength() == 2) {
                return "Int16";
            } else if (column.getLength() == 4) {
                return "Int32";
            }
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
            if (column.getLength() == 0) {
                return "String";
            } else {
                return "FixedString(" + column.getLength() + ")";
            }
        }

        throw new IllegalArgumentException(Strings.apply("The type %s (Property: %s) cannot be used in clickhouse!",
                                                         SchemaTool.getJdbcTypeName(type),
                                                         column.getSource()));
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
}
