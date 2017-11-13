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
import sirius.kernel.nls.NLS;

import java.sql.Types;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;

/**
 * Defines the dialect used to sync the schema against a MySQL database.
 */
@Register(name = "mysql", classes = DatabaseDialect.class)
public class MySQLDatabaseDialect extends BasicDatabaseDialect {

    private static final String KEY_TARGET = "target";
    private static final String KEY_CURRENT = "current";
    private static final String NOT_NULL = "NOT NULL";
    private static final String AUTO_INCREMENT = "AUTO_INCREMENT";

    @Override
    public String areColumnsEqual(TableColumn target, TableColumn current) {
        String reason = checkColumnSettings(target, current);
        if (reason != null) {
            return reason;
        }

        if (target.isNullable() != current.isNullable()) {
            // TIMESTAMP values cannot be null -> we gracefully ignore this
            // here, since the alter statement would be ignored anyway.
            if (target.isNullable() && target.getType() == Types.TIMESTAMP) {
                return null;
            }

            return NLS.get("MySQLDatabaseDialect.differentNull");
        }

        if (areDefaultsDifferent(target, current)) {
            return NLS.fmtr("MySQLDatabaseDialect.differentDefault")
                      .set(KEY_TARGET, target.getDefaultValue())
                      .set(KEY_CURRENT, current.getDefaultValue())
                      .format();
        }

        return null;
    }

    protected boolean areDefaultsDifferent(TableColumn target, TableColumn current) {
        return !equalValue(target.getDefaultValue(), current.getDefaultValue());
    }

    protected String checkColumnSettings(TableColumn target, TableColumn current) {
        if (!areTypesEqual(target.getType(), current.getType())) {
            return NLS.fmtr("MySQLDatabaseDialect.differentTypes")
                      .set(KEY_TARGET, SchemaTool.getJdbcTypeName(target.getType()))
                      .set(KEY_CURRENT, SchemaTool.getJdbcTypeName(current.getType()))
                      .format();
        }

        if (areTypesEqual(Types.CHAR, target.getType()) && !Strings.areEqual(target.getLength(), current.getLength())) {
            return NLS.fmtr("MySQLDatabaseDialect.differentLength")
                      .set(KEY_TARGET, target.getLength())
                      .set(KEY_CURRENT, current.getLength())
                      .format();
        }

        if (areTypesEqual(Types.DECIMAL, target.getType())) {
            if (!Strings.areEqual(target.getPrecision(), current.getPrecision())) {
                return NLS.fmtr("MySQLDatabaseDialect.differentPrecision")
                          .set(KEY_TARGET, target.getPrecision())
                          .set(KEY_CURRENT, current.getPrecision())
                          .format();
            }
            if (!Strings.areEqual(target.getScale(), current.getScale())) {
                return NLS.fmtr("MySQLDatabaseDialect.differentScale")
                          .set(KEY_TARGET, target.getScale())
                          .set(KEY_CURRENT, current.getScale())
                          .format();
            }
        }

        return null;
    }

    private boolean equalValue(String left, String right) {
        // Remove permutations...
        return checkForEquality(left, right) || checkForEquality(right, left);
    }

    private boolean checkForEquality(String left, String right) {
        if ("0".equals(left) && "0.0".equals(right)) {
            return true;
        }
        if ("1".equals(left) && "1.000".equals(right)) {
            return true;
        }
        if (left != null && ("'" + left + "'").equals(right)) {
            return true;
        }
        if (left == null && "''".equals(right)) {
            return true;
        }

        return Strings.areEqual(left, right);
    }

    @Override
    public String generateAddColumn(Table table, TableColumn col) {
        return MessageFormat.format("ALTER TABLE `{0}` ADD COLUMN `{1}` {2} {3} {4} {5}",
                                    table.getName(),
                                    col.getName(),
                                    getTypeName(col.getType(), col.getLength(), col.getPrecision(), col.getScale()),
                                    col.isNullable() ? "" : NOT_NULL,
                                    col.isAutoIncrement() ? AUTO_INCREMENT : "",
                                    getDefaultValueAsString(col));
    }

    private String getDefaultValueAsString(TableColumn col) {
        if (col.getDefaultValue() == null) {
            return "";
        }
        return "DEFAULT '" + col.getDefaultValue() + "'";
    }

    private boolean areTypesEqual(int type, int other) {
        if (type == other) {
            return true;
        }
        if (in(type, other, Types.BOOLEAN, Types.TINYINT, Types.BIT)) {
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

    private boolean in(int type, int other, int... types) {
        return Ints.contains(types, type) && Ints.contains(types, other);
    }

    private String getTypeName(int type, int length, int precision, int scale) {
        return convertNumericTypes(type, length, precision, scale);
    }

    private String convertNumericTypes(int type, int length, int precision, int scale) {
        if (Types.BIGINT == type) {
            return "BIGINT(20)";
        }
        if (Types.DOUBLE == type) {
            return "DOUBLE";
        }
        if (Types.DECIMAL == type) {
            return "DECIMAL(" + precision + "," + scale + ")";
        }
        if (Types.NUMERIC == type) {
            return "DECIMAL(" + precision + "," + scale + ")";
        }
        if (Types.FLOAT == type) {
            return "FLOAT";
        }
        if (Types.INTEGER == type) {
            return "INTEGER";
        }
        if (Types.BOOLEAN == type) {
            return "TINYINT(1)";
        }
        if (Types.BIT == type) {
            return "TINYINT(1)";
        }
        if (Types.TINYINT == type) {
            return "TINYINT";
        }
        return convertCharTypes(type, length);
    }

    private String convertCharTypes(int type, int length) {
        if (Types.CHAR == type) {
            return "VARCHAR(" + length + ")";
        }
        if (Types.VARCHAR == type) {
            return "VARCHAR(" + length + ")";
        }
        if (Types.CLOB == type) {
            return "LONGTEXT";
        }
        return convertTemporalTypes(type);
    }

    private String convertTemporalTypes(int type) {
        if (Types.DATE == type) {
            return "DATE";
        }
        if (Types.TIME == type) {
            return "TIME";
        }
        if (Types.TIMESTAMP == type) {
            return "TIMESTAMP";
        }

        return convertBinaryTypes(type);
    }

    private String convertBinaryTypes(int type) {
        if (Types.BLOB == type || Types.VARBINARY == type || Types.LONGVARBINARY == type) {
            return "LONGBLOB";
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
        return Collections.singletonList(MessageFormat.format(
                "ALTER TABLE `{0}` CHANGE COLUMN `{1}` `{2}` {3} {4} {5} {6}",
                table.getName(),
                name,
                toColumn.getName(),
                getTypeName(toColumn.getType(), toColumn.getLength(), toColumn.getPrecision(), toColumn.getScale()),
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
                                           getTypeName(col.getType(),
                                                       col.getLength(),
                                                       col.getPrecision(),
                                                       col.getScale()),
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
                sb.append(MessageFormat.format("   KEY `{0}` ({1}),\n", key.getName(),  String.join(", ", key.getColumns())));
            }
        }
        // We rely on the sync tool, to generate the constraints in the next run. Otherwise table with cross-references
        // cannot be created. Therefore only the PK is generated....
        sb.append(MessageFormat.format(" PRIMARY KEY ({0})\n) ENGINE=InnoDB",  String.join(", ", table.getPrimaryKey())));
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
}
