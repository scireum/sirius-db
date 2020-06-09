/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.schema;

import sirius.db.mixing.annotations.Length;
import sirius.db.mixing.properties.EnumProperty;
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
            if (shouldGenerateColumn(col)) {
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

    /**
     * Determines if the column should be generated / added to the target table.
     * <p>
     * This is a bit of an ugly hack to filter out the auto-generated
     * "id" column which is unsupported and also not required by clickhouse...
     *
     * @param col the column to check.
     * @return <tt>true</tt> if the column should be output / generated, <tt>false</tt> otherwise
     */
    private boolean shouldGenerateColumn(TableColumn col) {
        return !col.isAutoIncrement();
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

    @Nullable
    @Override
    public String generateRenameTable(Table table) {
        return MessageFormat.format("RENAME TABLE `{0}` TO `{1}`", table.getOldName(), table.getName());
    }

    /**
     * Determines the type for string columns.
     * <p>
     * {@link EnumProperty Enum properties} should use the type <tt>String</tt>. Only if the length is explicitly
     * given via the {@link Length} annotation we use <tt>FixedString</tt>.
     *
     * @param column the table column
     * @return the clickhouse type
     */
    private String getStringType(TableColumn column) {
        if (column.getSource() instanceof EnumProperty && !column.getSource()
                                                                 .getField()
                                                                 .isAnnotationPresent(Length.class)) {
            return "String";
        }

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
    protected boolean areColumnLengthsEqual(TableColumn target, TableColumn current) {
        // Enum columns have a length specified, but we create them as unbounded strings by default
        // therefore we should skip the change here.
        return target.getLength() == 0 || current.getLength() == 0 || super.areColumnLengthsEqual(target, current);
    }

    @Override
    protected boolean areTypesEqual(int type, int other) {
        if (type == other) {
            return true;
        }
        if (in(type, other, Types.BOOLEAN, Types.INTEGER)) {
            return true;
        }
        if (in(type, other, Types.VARCHAR, Types.CHAR)) {
            return true;
        }

        return in(type, other, Types.NUMERIC, Types.DECIMAL);
    }

    @Override
    protected boolean areDefaultsDifferent(TableColumn target, TableColumn current) {
        return false;
    }

    @Override
    public List<String> generateAlterColumnTo(Table table, @Nullable String oldName, TableColumn toColumn) {
        if (Strings.isFilled(oldName) && !Strings.areEqual(oldName, toColumn.getName())) {
            return Collections.singletonList(MessageFormat.format("ALTER TABLE `{0}` RENAME COLUMN `{1}` TO `{2}`",
                                                                  table.getName(),
                                                                  oldName,
                                                                  toColumn.getName()));
        } else {
            return Collections.singletonList(MessageFormat.format("ALTER TABLE `{0}` MODIFY COLUMN `{1}` {2} {3}",
                                                                  table.getName(),
                                                                  toColumn.getName(),
                                                                  getTypeName(toColumn),
                                                                  getDefaultValueAsString(toColumn)));
        }
    }

    @Override
    public String generateAddColumn(Table table, TableColumn col) {
        if (!shouldGenerateColumn(col)) {
            return null;
        }
        return MessageFormat.format("ALTER TABLE `{0}` ADD COLUMN `{1}` {2} {3}",
                                    table.getName(),
                                    col.getName(),
                                    getTypeName(col),
                                    getDefaultValueAsString(col));
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
