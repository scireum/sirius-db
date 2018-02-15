/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.schema;

import com.google.common.primitives.Ints;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Register;

import java.sql.Types;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;

/**
 * Defines the dialect used to sync the schema against a HSQL database which is used in tests.
 */
@Register(name = "hsqldb", classes = DatabaseDialect.class)
public class HSQLDBDatabaseDialect extends BasicDatabaseDialect {

    private static final String NOT_NULL = "NOT NULL";
    private static final String IDENTITY = "IDENTITY";
    private static final String MSG_COLUMNS_DIFFER = "The column definitions differ";

    @Override
    public String areColumnsEqual(TableColumn target, TableColumn current) {
        if (checkColumnSettings(target, current)) {
            return MSG_COLUMNS_DIFFER;
        }

        if (target.isNullable() != current.isNullable() && (target.getType() != Types.TIMESTAMP
                                                            || target.getDefaultValue() != null)) {
            return MSG_COLUMNS_DIFFER;
        }

        if (checkDefaultValue(target, current)) {
            return MSG_COLUMNS_DIFFER;
        }

        return null;
    }

    protected boolean checkColumnSettings(TableColumn target, TableColumn current) {
        if (!areTypesEqual(target.getType(), current.getType())) {
            return true;
        }

        if (areTypesEqual(Types.CHAR, target.getType()) && !Strings.areEqual(target.getLength(), current.getLength())) {
            return true;
        }

        if (areTypesEqual(Types.DECIMAL, target.getType())) {
            if (!Strings.areEqual(target.getPrecision(), current.getPrecision())) {
                return true;
            }
            if (!Strings.areEqual(target.getScale(), current.getScale())) {
                return true;
            }
        }

        return false;
    }

    protected boolean checkDefaultValue(TableColumn target, TableColumn current) {
        return !equalValue(target.getDefaultValue(), current.getDefaultValue());
    }

    private boolean equalValue(String left, String right) {
        // Remove permutations...
        return checkForEquality(left, right) || checkForEquality(right, left);
    }

    private boolean checkForEquality(String left, String right) {
        if ("0".equals(left) && "0.0".equals(right)) {
            return true;
        }
        if (left == null && "''".equals(right)) {
            return true;
        }

        return Strings.areEqual(left, right);
    }

    @Override
    public String generateAddColumn(Table table, TableColumn col) {
        return MessageFormat.format("ALTER TABLE {0} ADD COLUMN {1} {2} {3} {4} {5}",
                                    table.getName(),
                                    col.getName(),
                                    getTypeName(col.getType(), col.getLength(), col.getPrecision(), col.getScale()),
                                    col.isNullable() ? "" : NOT_NULL,
                                    col.isAutoIncrement() ? IDENTITY : "",
                                    getDefaultValueAsString(col));
    }

    private String getDefaultValueAsString(TableColumn col) {
        if (col.getDefaultValue() == null) {
            return "";
        }

        if (isNeedsQuotation(col)) {
            return "DEFAULT '" + col.getDefaultValue() + "'";
        } else {
            return "DEFAULT " + col.getDefaultValue();
        }
    }

    private boolean isNeedsQuotation(TableColumn col) {
        return col.getType() != Types.BIGINT
               && col.getType() != Types.DECIMAL
               && col.getType() != Types.CLOB
               && col.getType() != Types.INTEGER
               && col.getType() != Types.TINYINT
               && col.getType() != Types.BOOLEAN
               && col.getType() != Types.DOUBLE
               && col.getType() != Types.FLOAT
               && col.getType() != Types.SMALLINT
               && col.getType() != Types.BLOB
               && col.getType() != Types.NUMERIC;
    }

    private boolean areTypesEqual(int type, int other) {
        if (type == other) {
            return true;
        }
        return in(type, other, Types.BOOLEAN, Types.TINYINT, Types.BIT, Types.SMALLINT)
               || in(type,
                     other,
                     Types.VARCHAR,
                     Types.CHAR)
               || in(type,
                     other,
                     Types.LONGVARCHAR,
                     Types.CLOB)
               || in(type, other, Types.LONGVARBINARY, Types.BLOB, Types.VARBINARY);
    }

    private boolean in(int type, int other, int... types) {
        return Ints.contains(types, type) && Ints.contains(types, other);
    }

    private String getTypeName(int type, int length, int precision, int scale) {
        return convertNumericType(type, length, precision, scale);
    }

    private String convertNumericType(int type, int length, int precision, int scale) {
        if (Types.BIGINT == type) {
            return "BIGINT";
        }
        if (Types.BOOLEAN == type || Types.BIT == type) {
            return "BOOLEAN";
        }
        if (Types.DOUBLE == type) {
            return "DOUBLE";
        }
        if (Types.DECIMAL == type) {
            return "DECIMAL(" + precision + "," + scale + ")";
        }
        if (Types.FLOAT == type) {
            return "FLOAT";
        }
        if (Types.INTEGER == type) {
            return "INTEGER";
        }

        return convertCharacterType(type, length);
    }

    private String convertCharacterType(int type, int length) {
        if (Types.VARCHAR == type) {
            return "VARCHAR(" + length + ")";
        }
        if (Types.CHAR == type) {
            return "VARCHAR(" + length + ")";
        }
        if (Types.CLOB == type) {
            return "CLOB";
        }

        return convertTemporalType(type);
    }

    private String convertTemporalType(int type) {
        if (Types.DATE == type) {
            return "DATE";
        }
        if (Types.TIME == type) {
            return "TIME";
        }
        if (Types.TIMESTAMP == type) {
            return "TIMESTAMP";
        }
        if (Types.TINYINT == type) {
            return "SMALLINT";
        }

        return convertBinaryType(type);
    }

    private String convertBinaryType(int type) {
        if (Types.BLOB == type) {
            return "BLOB";
        }
        if (Types.VARBINARY == type) {
            return "BLOB";
        }
        if (Types.LONGVARBINARY == type) {
            return "BLOB";
        }

        throw new IllegalArgumentException(Strings.apply("The type %s cannot be used as JDBC type!",
                                                         SchemaTool.getJdbcTypeName(type)));
    }

    @Override
    public List<String> generateAlterColumnTo(Table table, String oldName, TableColumn toColumn) {
        String name = oldName;
        if (name == null) {
            name = toColumn.getName();
        }
        return Collections.singletonList(MessageFormat.format("ALTER TABLE {0} CHANGE COLUMN {1} {2} {3} {4} {5} {6}",
                                                              table.getName(),
                                                              name,
                                                              toColumn.getName(),
                                                              getTypeName(toColumn.getType(),
                                                                          toColumn.getLength(),
                                                                          toColumn.getPrecision(),
                                                                          toColumn.getScale()),
                                                              getDefaultValueAsString(toColumn),
                                                              toColumn.isNullable() ? "" : NOT_NULL,
                                                              toColumn.isAutoIncrement() ? IDENTITY : ""));
    }

    @Override
    public String generateCreateTable(Table table) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ");
        sb.append(table.getName());
        sb.append(" (\n");
        boolean hasIdentityColumn = false;
        boolean first = true;
        for (TableColumn col : table.getColumns()) {
            if (col.isAutoIncrement()) {
                hasIdentityColumn = true;
            }
            if (!first) {
                sb.append(",\n");
            }
            first = false;
            sb.append(MessageFormat.format("  {0} {1} {2} {3} {4}",
                                           col.getName(),
                                           getTypeName(col.getType(),
                                                       col.getLength(),
                                                       col.getPrecision(),
                                                       col.getScale()),
                                           getDefaultValueAsString(col),
                                           col.isNullable() ? "" : NOT_NULL,
                                           col.isAutoIncrement() ? IDENTITY : ""));
        }
        for (Key key : table.getKeys()) {
            if (key.isUnique()) {
                sb.append(MessageFormat.format(",\n   CONSTRAINT {0} UNIQUE ({1})",
                                               key.getName(),
                                               String.join(", ", key.getColumns())));
            }
        }

        // We rely on the sync tool, to generate the constraints in the next run. Otherwise table with cross-references
        // cannot be created. Therefore only the PK is generated....
        if (!hasIdentityColumn) {
            sb.append(MessageFormat.format(",\n PRIMARY KEY ({0})", String.join(", ", table.getPrimaryKey())));
        }
        sb.append("\n)");
        return sb.toString();
    }

    @Override
    public String generateDropColumn(Table table, TableColumn col) {
        return MessageFormat.format("ALTER TABLE {0} DROP COLUMN {1}", table.getName(), col.getName());
    }

    @Override
    public String generateDropForeignKey(Table table, ForeignKey key) {
        return MessageFormat.format("ALTER TABLE {0} DROP FOREIGN KEY {1}", table.getName(), key.getName());
    }

    @Override
    public String generateDropKey(Table table, Key key) {
        return MessageFormat.format("ALTER TABLE {0} DROP INDEX {1}", table.getName(), key.getName());
    }

    @Override
    public String generateDropTable(Table table) {
        return MessageFormat.format("DROP TABLE {0} ", table.getName());
    }

    @Override
    public String translateColumnName(String name) {
        return name.toUpperCase();
    }

    @Override
    public boolean isColumnCaseSensitive() {
        return false;
    }

    @Override
    public boolean shouldDropKey(Table targetTable, Table currentTable, Key key) {
        return !key.getName().startsWith("SQL");
    }

    @Override
    public String getEffectiveKeyName(Table targetTable, Key key) {
        return targetTable.getName() + "_" + key.getName();
    }
}
