/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.schema;

import com.google.common.primitives.Ints;
import sirius.db.jdbc.OMA;
import sirius.db.mixing.annotations.Engine;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.nls.NLS;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
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

    protected static final String KEY_TARGET = "target";
    protected static final String KEY_CURRENT = "current";
    protected static final int DEFAULT_MAX_CONSTRAINT_NAME_LENGTH = 1024;

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

    @SuppressWarnings("squid:S1067")
    @Explain("We rather have all checks in one place.")
    protected boolean hasEscapedDefaultValue(TableColumn col) {
        return Types.CHAR == col.getType()
               || Types.VARCHAR == col.getType()
               || Types.CLOB == col.getType()
               || Types.DATE == col.getType()
               || Types.TIMESTAMP == col.getType()
               || Types.LONGVARCHAR == col.getType()
               || Types.TIME == col.getType();
    }

    @Override
    public String generateAddForeignKey(Table table, ForeignKey key) {
        return MessageFormat.format("ALTER TABLE `{0}` ADD CONSTRAINT `{1}` FOREIGN KEY ({2}) REFERENCES `{3}` ({4})",
                                    table.getName(),
                                    Strings.limit(key.getName(), getConstraintCharacterLimit()),
                                    String.join(", ", key.getColumns()),
                                    key.getForeignTable(),
                                    String.join(", ", key.getForeignColumns()));
    }

    @Override
    public String generateAddKey(Table table, Key key) {
        if (key.isUnique()) {
            return MessageFormat.format("ALTER TABLE `{0}` ADD CONSTRAINT `{1}` UNIQUE ({2})",
                                        table.getName(),
                                        key.getName(),
                                        String.join(", ", key.getColumns()));
        } else {
            return MessageFormat.format("ALTER TABLE `{0}` ADD INDEX `{1}` ({2})",
                                        table.getName(),
                                        key.getName(),
                                        String.join(", ", key.getColumns()));
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
                String.join(", ", table.getPrimaryKey())));
    }

    @Override
    public String generateDropColumn(Table table, TableColumn col) {
        return MessageFormat.format("ALTER TABLE `{0}` DROP COLUMN `{1}`", table.getName(), col.getName());
    }

    @Override
    public String generateDropForeignKey(Table table, ForeignKey key) {
        return MessageFormat.format("ALTER TABLE `{0}` DROP FOREIGN KEY `{1}`",
                                    table.getName(),
                                    Strings.limit(key.getName(), getConstraintCharacterLimit()));
    }

    @Override
    public String generateDropKey(Table table, Key key) {
        return MessageFormat.format("ALTER TABLE `{0}` DROP INDEX `{1}`", table.getName(), key.getName());
    }

    @Override
    public String generateDropTable(Table table) {
        return MessageFormat.format("DROP TABLE `{0}` ", table.getName());
    }

    @Nullable
    @Override
    public String generateRenameTable(Table table) {
        return MessageFormat.format("ALTER TABLE `{0}` RENAME `{1}`", table.getOldName(), table.getName());
    }

    @Override
    public String translateColumnName(String name) {
        return name;
    }

    @SuppressWarnings({"squid:S3776", "squid:MethodCyclomaticComplexity"})
    @Explain("We rather have all mappings in one place, even if the complexity is too high")
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

    @Override
    public String areColumnsEqual(TableColumn target, TableColumn current) {
        String reason = checkColumnSettings(target, current);
        if (reason != null) {
            return reason;
        }

        if (target.isNullable() != current.isNullable() && (target.getType() != Types.TIMESTAMP
                                                            || target.getDefaultValue() != null)) {
            return NLS.get("BasicDatabaseDialect.differentNull");
        }

        if (areDefaultsDifferent(target, current)) {
            return NLS.fmtr("BasicDatabaseDialect.differentDefault")
                      .set(KEY_TARGET, target.getDefaultValue())
                      .set(KEY_CURRENT, current.getDefaultValue())
                      .format();
        }

        return null;
    }

    protected boolean areDefaultsDifferent(TableColumn target, TableColumn current) {
        if (equalValue(target.getDefaultValue(), current.getDefaultValue())) {
            return false;
        }

        // TIMESTAMP values cannot be null -> we gracefully ignore this
        // here, sice the alter statement would be ignored anyway.
        return target.getType() != Types.TIMESTAMP || target.getDefaultValue() != null;
    }

    protected String checkColumnSettings(TableColumn target, TableColumn current) {
        if (!areTypesEqual(target.getType(), current.getType())) {
            return NLS.fmtr("BasicDatabaseDialect.differentTypes")
                      .set(KEY_TARGET, SchemaTool.getJdbcTypeName(target.getType()))
                      .set(KEY_CURRENT, SchemaTool.getJdbcTypeName(current.getType()))
                      .format();
        }

        if (!areColumnLengthsEqual(target, current)) {
            return NLS.fmtr("BasicDatabaseDialect.differentLength")
                      .set(KEY_TARGET, target.getLength())
                      .set(KEY_CURRENT, current.getLength())
                      .format();
        }

        if (areTypesEqual(Types.DECIMAL, target.getType())) {
            if (!Strings.areEqual(target.getPrecision(), current.getPrecision())) {
                return NLS.fmtr("BasicDatabaseDialect.differentPrecision")
                          .set(KEY_TARGET, target.getPrecision())
                          .set(KEY_CURRENT, current.getPrecision())
                          .format();
            }
            if (!Strings.areEqual(target.getScale(), current.getScale())) {
                return NLS.fmtr("BasicDatabaseDialect.differentScale")
                          .set(KEY_TARGET, target.getScale())
                          .set(KEY_CURRENT, current.getScale())
                          .format();
            }
        }

        return null;
    }

    protected boolean areColumnLengthsEqual(TableColumn target, TableColumn current) {
        // The length is only enforced for CHAR fields by default...
        if (!areTypesEqual(Types.CHAR, target.getType())) {
            return true;
        }

        return Strings.areEqual(target.getLength(), current.getLength());
    }

    protected boolean equalValue(String a, String b) {
        // Remove permutations...
        return checkForEquality(a, b) || checkForEquality(b, a);
    }

    protected boolean checkForEquality(String left, String right) {
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

    protected String getDefaultValueAsString(TableColumn col) {
        if (col.getDefaultValue() == null) {
            return "";
        }

        if (isNeedsQuotation(col)) {
            return "DEFAULT '" + col.getDefaultValue() + "'";
        } else {
            return "DEFAULT " + col.getDefaultValue();
        }
    }

    @SuppressWarnings("squid:S1067")
    @Explain("We rather have all mappings in one place, even if the complexity is too high")
    protected boolean isNeedsQuotation(TableColumn col) {
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

    protected boolean areTypesEqual(int type, int other) {
        return type == other;
    }

    protected boolean in(int type, int other, int... types) {
        return Ints.contains(types, type) && Ints.contains(types, other);
    }

    @SuppressWarnings({"squid:S3776", "squid:MethodCyclomaticComplexity"})
    @Explain("We rather have all mappings in one place, even if the complexity is too high")
    protected String getTypeName(TableColumn column) {
        int type = column.getType();
        if (Types.BIGINT == type) {
            return "BIGINT(20)";
        }
        if (Types.DOUBLE == type) {
            return "DOUBLE";
        }
        if (Types.DECIMAL == type) {
            return "DECIMAL(" + column.getPrecision() + "," + column.getScale() + ")";
        }
        if (Types.NUMERIC == type) {
            return "DECIMAL(" + column.getPrecision() + "," + column.getScale() + ")";
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
        if (Types.CHAR == type) {
            return "CHAR(" + ensurePositiveLength(column, 255) + ")";
        }
        if (Types.VARCHAR == type) {
            return "VARCHAR(" + ensurePositiveLength(column, 255) + ")";
        }
        if (Types.CLOB == type) {
            return "LONGTEXT";
        }
        if (Types.DATE == type) {
            return "DATE";
        }
        if (Types.TIME == type) {
            return "TIME";
        }
        if (Types.TIMESTAMP == type) {
            return "TIMESTAMP";
        }
        if (Types.BLOB == type || Types.VARBINARY == type || Types.LONGVARBINARY == type) {
            return "LONGBLOB";
        }

        throw new IllegalArgumentException(Strings.apply("The type %s (Property: %s) cannot be used as JDBC type!",
                                                         SchemaTool.getJdbcTypeName(type),
                                                         column.getSource()));
    }

    protected int ensurePositiveLength(TableColumn column, int defaultValue) {
        if (column.getLength() == 0) {
            OMA.LOG.WARN("The property '%s' doesn't specify a length for its column! Defaulting to %s!",
                         column.getSource(),
                         defaultValue);
            return defaultValue;
        }

        return column.getLength();
    }

    @Override
    public String getEffectiveKeyName(Table targetTable, Key key) {
        return key.getName();
    }

    protected Value getEngine(Table table) {
        return table.getSource().getAnnotation(Engine.class).map(Engine::value).map(Value::of).orElse(Value.EMPTY);
    }

    /**
     * Determines the length a constraint should have.
     *
     * @return the maximum allowed length of a constraint
     */
    protected int getConstraintCharacterLimit() {
        return DEFAULT_MAX_CONSTRAINT_NAME_LENGTH;
    }

    @Override
    public String getDefaultValue(ResultSet rs) throws SQLException {
        return rs.getString("COLUMN_DEF");
    }
}
