/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.schema;

import sirius.kernel.commons.Strings;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Provides a base implementation of {@link DatabaseDialect}.
 */
public abstract class BasicDatabaseDialect implements DatabaseDialect {

    @Override
    public int getJDBCType(Class<?> clazz) {
        return resolveType(clazz);
    }

    @Override
    public Table completeTableInfos(Table table) {
        for (TableColumn col : table.getColumns()) {
            if (col.getDefaultValue() != null && hasEscapedDefaultValue(col)) {
                col.setDefaultValue("'" + col.getDefaultValue() + "'");
            }
        }
        // The PK is also identified as INDEX...
        Key key = table.getKey("PRIMARY");
        if (key != null) {
            table.getKeys().remove(key);
        }
        return table;
    }

    protected boolean hasEscapedDefaultValue(TableColumn col) {
        if (Types.CHAR == col.getType() || Types.VARCHAR == col.getType() || Types.CLOB == col.getType()) {
            return true;
        }

        return Types.DATE == col.getType()
               || Types.TIMESTAMP == col.getType()
               || Types.LONGVARCHAR == col.getType()
               || Types.TIME == col.getType();
    }

    protected String listToString(List<String> columns) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String col : columns) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(col);
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
        if (key.isUnique()) {
            return MessageFormat.format("ALTER TABLE `{0}` ADD CONSTRAINT `{1}` UNIQUE ({2})",
                                        table.getName(),
                                        key.getName(),
                                        listToString(key.getColumns()));
        } else {
            return MessageFormat.format("ALTER TABLE `{0}` ADD INDEX `{1}` ({2})",
                                        table.getName(),
                                        key.getName(),
                                        listToString(key.getColumns()));
        }
    }

    @Override
    public List<String> generateAlterForeignKey(Table table, ForeignKey from, ForeignKey to) {
        List<String> actions = new ArrayList<>();
        if (from != null) {
            actions.add(generateDropForeignKey(table, from));
        }
        actions.add(generateAddForeignKey(table, to));
        return actions;
    }

    @Override
    public List<String> generateAlterKey(Table table, Key from, Key to) {
        List<String> actions = new ArrayList<>();
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
    public String generateDropColumn(Table table, TableColumn col) {
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
    public String translateColumnName(String name) {
        return name;
    }

    protected int resolveType(Class<?> clazz) {
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

        return resolveTemporalType(clazz);
    }

    private int resolveTemporalType(Class<?> clazz) {
        if (Date.class.equals(clazz)) {
            return Types.DATE;
        }
        if (Time.class.equals(clazz)) {
            return Types.TIME;
        }
        if (Timestamp.class.equals(clazz)) {
            return Types.TIMESTAMP;
        }

        return resolveBinaryType(clazz);
    }

    private int resolveBinaryType(Class<?> clazz) {
        if (Clob.class.equals(clazz)) {
            return Types.CLOB;
        }
        if (Blob.class.equals(clazz)) {
            return Types.BLOB;
        }

        return resolvePrimitiveType(clazz);
    }

    private int resolvePrimitiveType(Class<?> clazz) {
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

        throw new IllegalArgumentException(Strings.apply("The class $s cannot be converted into a JDBC type!", clazz));
    }
}
