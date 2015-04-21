package sirius.mixing.schema;

import com.google.common.primitives.Ints;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Register;
import sirius.kernel.nls.NLS;

import java.math.BigDecimal;
import java.sql.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Register(name = "mysql")
public class MySQLDatabaseDialect implements DatabaseDialect {

    @Override
    public String areColumnsEqual(Column target, Column current) {
        if (!areTypesEqual(target.getType(), current.getType())) {
            return NLS.fmtr("scireum.db.schema.mysql.differentTypes")
                      .set("target", SchemaTool.getJdbcTypeName(target.getType()))
                      .set("current", SchemaTool.getJdbcTypeName(current.getType()))
                      .format();
        }
        if (target.isNullable() != current.isNullable()) {
            if (target.getType() != Types.TIMESTAMP || target.getDefaultValue() != null) {
                return NLS.get("scireum.db.schema.mysql.differentNull");
            }
        }
        if (!equalValue(target.getDefaultValue(), current.getDefaultValue())) {
            // TIMESTAMP values cannot be null -> we gracefully ignore this
            // here, sice the alter statement would be ignored anyway.
            if (target.getType() != Types.TIMESTAMP || target.getDefaultValue() != null) {
                return NLS.fmtr("scireum.db.schema.mysql.differentDefault")
                          .set("target", target.getDefaultValue())
                          .set("current", current.getDefaultValue())
                          .format();
            }
        }
        if (areTypesEqual(Types.CHAR, target.getType())) {
            if (!Strings.areEqual(target.getLength(), current.getLength())) {
                return NLS.fmtr("scireum.db.schema.mysql.differentLength")
                          .set("target", target.getLength())
                          .set("current", current.getLength())
                          .format();
            }
        }
        if (areTypesEqual(Types.DECIMAL, target.getType())) {
            if (!Strings.areEqual(target.getPrecision(), current.getPrecision())) {
                return NLS.fmtr("scireum.db.schema.mysql.differentPrecision")
                          .set("target", target.getPrecision())
                          .set("current", current.getPrecision())
                          .format();
            }
            if (!Strings.areEqual(target.getScale(), current.getScale())) {
                return NLS.fmtr("scireum.db.schema.mysql.differentScale")
                          .set("target", target.getScale())
                          .set("current", current.getScale())
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
    public Table completeTableInfos(Table table) {
        for (Column col : table.getColumns()) {
            if (Types.CHAR == col.getType() || Types.VARCHAR == col.getType() || Types.CLOB == col.getType() || Types.DATE == col
                    .getType() || Types.TIMESTAMP == col.getType() || Types.LONGVARCHAR == col.getType() || Types.TIME == col
                    .getType()) {
                col.setDefaultValue(col.getDefaultValue() == null ? null : "'" + col.getDefaultValue() + "'");
            }
        }
        // The PK is also identified as INDEX...
        Key key = table.getKey("PRIMARY");
        if (key != null) {
            table.getKeys().remove(key);
        }
        return table;
    }

    @Override
    public String generateAddColumn(Table table, Column col) {
        return MessageFormat.format("ALTER TABLE `{0}` ADD COLUMN `{1}` {2} {3} {4} {5}",
                                    table.getName(),
                                    col.getName(),
                                    getTypeName(col.getType(), col.getLength(), col.getPrecision(), col.getScale()),
                                    col.isNullable() ? "" : "NOT NULL",
                                    col.isAutoIncrement() ? "AUTO_INCREMENT" : "",
                                    getDefaultValueAsString(col));
    }

    private String getDefaultValueAsString(Column col) {
        if (col.getDefaultValue() == null) {
            return "";
        }
        return "DEFAULT '" + col.getDefaultValue() + "'";
    }

    private boolean areTypesEqual(int type, int other) {
        if (type == other) {
            return true;
        }
        return in(type, other, Types.BOOLEAN, Types.TINYINT, Types.BIT) || in(type,
                                                                              other,
                                                                              Types.VARCHAR,
                                                                              Types.CHAR) || in(type,
                                                                                                other,
                                                                                                Types.LONGVARCHAR,
                                                                                                Types.CLOB) || in(type,
                                                                                                                  other,
                                                                                                                  Types.LONGVARBINARY,
                                                                                                                  Types.BLOB,
                                                                                                                  Types.VARBINARY) || in(
                type,
                other,
                Types.NUMERIC,
                Types.DECIMAL);
    }

    private boolean in(int type, int other, int... types) {
        return Ints.contains(types, type) && Ints.contains(types, other);
    }

    private String getTypeName(int type, int length, int precision, int scale) {
        if (Types.BIGINT == type) {
            return "BIGINT(20)";
        }
        if (Types.BLOB == type) {
            return "LONGBLOB";
        }
        if (Types.VARBINARY == type) {
            return "LONGBLOB";
        }
        if (Types.LONGVARBINARY == type) {
            return "LONGBLOB";
        }
        if (Types.BOOLEAN == type) {
            return "TINYINT(1)";
        }
        if (Types.BIT == type) {
            return "TINYINT(1)";
        }
        if (Types.CHAR == type) {
            return "VARCHAR(" + length + ")";
        }
        if (Types.CLOB == type) {
            return "LONGTEXT";
        }
        if (Types.DATE == type) {
            return "DATE";
        }
        if (Types.DOUBLE == type) {
            return "DOUBLE";
        }
        if (Types.DECIMAL == type) {
            return "DECIMAL(" + scale + "," + precision + ")";
        }
        if (Types.NUMERIC == type) {
            return "DECIMAL(" + scale + "," + precision + ")";
        }
        if (Types.FLOAT == type) {
            return "FLOAT";
        }
        if (Types.INTEGER == type) {
            return "INTEGER";
        }
        if (Types.TIME == type) {
            return "TIME";
        }
        if (Types.TIMESTAMP == type) {
            return "TIMESTAMP";
        }
        if (Types.TINYINT == type) {
            return "TINYINT";
        }
        if (Types.VARCHAR == type) {
            return "VARCHAR(" + length + ")";
        }
        throw new IllegalArgumentException(NLS.fmtr("scireum.db.schema.mysql.unknownType")
                                              .set("type", SchemaTool.getJdbcTypeName(type))
                                              .format());
    }

    private String listToString(List<String> columns) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String col : columns) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append("`");
            sb.append(col);
            sb.append("`");
        }
        return sb.toString();
    }

    @Override
    public String generateAddForeignKey(Table table, ForeignKey key) {
        return MessageFormat.format("ALTER TABLE `{0}` ADD CONSTRAINT `{1}` FOREIGN KEY ({2}) REFERENCES `{3}` ({4})",
                                    table.getName(),
                                    key.getName(),
                                    listToString(key.getColumns()),
                                    key.getForeignTable(),
                                    listToString(key.getForeignColumns()));
    }

    @Override
    public String generateAddKey(Table table, Key key) {
        return MessageFormat.format("ALTER TABLE `{0}` ADD INDEX `{1}` ({2})",
                                    table.getName(),
                                    key.getName(),
                                    listToString(key.getColumns()));
    }

    @Override
    public List<String> generateAlterColumnTo(Table table, String oldName, Column toColumn) {
        if (oldName == null) {
            oldName = toColumn.getName();
        }
        return Collections.singletonList(MessageFormat.format(
                "ALTER TABLE `{0}` CHANGE COLUMN `{1}` `{2}` {3} {4} {5} {6}",
                table.getName(),
                oldName,
                toColumn.getName(),
                getTypeName(toColumn.getType(), toColumn.getLength(), toColumn.getPrecision(), toColumn.getScale()),
                toColumn.isNullable() ? "" : "NOT NULL",
                toColumn.isAutoIncrement() ? "AUTO_INCREMENT" : "",
                getDefaultValueAsString(toColumn)));
    }

    @Override
    public List<String> generateAlterForeignKey(Table table, ForeignKey from, ForeignKey to) {
        List<String> actions = new ArrayList<String>();
        if (from != null) {
            actions.add(generateDropForeignKey(table, from));
        }
        actions.add(generateAddForeignKey(table, to));
        return actions;
    }

    @Override
    public List<String> generateAlterKey(Table table, Key from, Key to) {
        List<String> actions = new ArrayList<String>();
        if (from != null) {
            actions.add(generateDropKey(table, from));
        }
        actions.add(generateAddKey(table, to));
        return actions;
    }

    @Override
    public List<String> generateAlterPrimaryKey(Table table) {
        return Collections.singletonList(MessageFormat.format(
                "ALTER TABLE `{0}` DROP PRIMARY KEY, ADD PRIMARY KEY ({1})",
                table.getName(),
                listToString(table.getPrimaryKey())));
    }

    @Override
    public String generateCreateTable(Table table) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE `");
        sb.append(table.getName());
        sb.append("` (\n");
        for (Column col : table.getColumns()) {
            sb.append(MessageFormat.format("  `{0}` {1} {2} {3} {4},\n",
                                           col.getName(),
                                           getTypeName(col.getType(),
                                                       col.getLength(),
                                                       col.getPrecision(),
                                                       col.getScale()),
                                           col.isNullable() ? "" : "NOT NULL",
                                           col.isAutoIncrement() ? "AUTO_INCREMENT" : "",
                                           getDefaultValueAsString(col)));
        }
        for (Key key : table.getKeys()) {
            sb.append(MessageFormat.format("   KEY `{0}` ({1}),\n", key.getName(), listToString(key.getColumns())));
        }
        // AHA 02.11.11 - We rely on the sync tool, to generate the constraints
        // in the next run. Otherwise table with cross-references cannot be
        // created.
        // for (ForeignKey key : table.getForeignKeys()) {
        // sb.append(MessageFormat
        // .format("   CONSTRAINT `{0}` FOREIGN KEY ({1}) REFERENCES `{2}` ({3}),\n",
        // key.getName(), listToString(key.getColumns()),
        // key.getForeignTable(),
        // listToString(key.getForeignColumns())));
        // }
        sb.append(MessageFormat.format(" PRIMARY KEY ({0})\n) ENGINE=InnoDB", listToString(table.getPrimaryKey())));
        return sb.toString();
    }

    @Override
    public String generateDropColumn(Table table, Column col) {
        return MessageFormat.format("ALTER TABLE `{0}` DROP COLUMN `{1}`", table.getName(), col.getName());
    }

    @Override
    public String generateDropForeignKey(Table table, ForeignKey key) {
        return MessageFormat.format("ALTER TABLE `{0}` DROP FOREIGN KEY `{1}`", table.getName(), key.getName());
    }

    @Override
    public String generateDropKey(Table table, Key key) {
        return MessageFormat.format("ALTER TABLE `{0}` DROP INDEX `{1}`", table.getName(), key.getName());

    }

    @Override
    public String generateDropTable(Table table) {
        return MessageFormat.format("DROP TABLE `{0}` ", table.getName());
    }

    @Override
    public int getJDBCType(Class<?> clazz) {
        if (String.class.equals(clazz)) {
            return Types.VARCHAR;
        }
        if (Integer.class.equals(clazz)) {
            return Types.INTEGER;
        }
        if (Long.class.equals(clazz)) {
            return Types.BIGINT;
        }
        if (Double.class.equals(clazz)) {
            return Types.DOUBLE;
        }
        if (BigDecimal.class.equals(clazz)) {
            return Types.DECIMAL;
        }
        if (Float.class.equals(clazz)) {
            return Types.FLOAT;
        }
        if (Boolean.class.equals(clazz)) {
            return Types.TINYINT;
        }
        if (int.class.equals(clazz)) {
            return Types.INTEGER;
        }
        if (long.class.equals(clazz)) {
            return Types.BIGINT;
        }
        if (double.class.equals(clazz)) {
            return Types.DOUBLE;
        }
        if (float.class.equals(clazz)) {
            return Types.FLOAT;
        }
        if (boolean.class.equals(clazz)) {
            return Types.TINYINT;
        }
        if (Date.class.equals(clazz)) {
            return Types.DATE;
        }
        if (Time.class.equals(clazz)) {
            return Types.TIME;
        }
        if (Timestamp.class.equals(clazz)) {
            return Types.TIMESTAMP;
        }
        if (Clob.class.equals(clazz)) {
            return Types.CLOB;
        }
        if (Blob.class.equals(clazz)) {
            return Types.BLOB;
        }
        throw new IllegalArgumentException(NLS.fmtr("scireum.db.schema.mysql.invalidType").set("type", clazz).format());
    }

    @Override
    public String translateColumnName(String name) {
        return name;
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
