/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.constraints;

import sirius.db.jdbc.Databases;
import sirius.db.mixing.Mapping;
import sirius.db.jdbc.Constraint;
import sirius.db.jdbc.SmartQuery;

/**
 * Represents a relational operator applied on a field.
 */
public class FieldOperator extends Constraint {

    /**
     * Enumerates all supported operators.
     */
    public enum Operator {
        LT("<"), LT_EQ("<="), EQ("="), GT_EQ(">="), GT(">"), NE("<>");

        private final String sql;

        Operator(String sql) {
            this.sql = sql;
        }

        @Override
        public String toString() {
            return sql;
        }
    }

    private Mapping field;
    private Object value;
    private Operator op;
    private boolean ignoreNull;
    private boolean orNull;

    private FieldOperator(Mapping field) {
        this.field = field;
    }

    /**
     * Creates a new constraint on the given field.
     *
     * @param field the field to filter on
     * @return the constraint applied on the given field
     */
    public static FieldOperator on(Mapping field) {
        return new FieldOperator(field);
    }

    /**
     * Generates an equals constraint like {@code field = value}.
     *
     * @param value the value to compare against
     * @return the constraint itself
     */
    public FieldOperator eq(Object value) {
        this.value = value;
        this.op = Operator.EQ;
        return this;
    }

    /**
     * Generates an equals constraint like {@code field &lt; value}.
     *
     * @param value the value to compare against
     * @return the constraint itself
     */
    public FieldOperator lessThan(Object value) {
        this.value = value;
        this.op = Operator.LT;
        return this;
    }

    /**
     * Generates an equals constraint like {@code field &lt;= value}.
     *
     * @param value the value to compare against
     * @return the constraint itself
     */
    public FieldOperator lessOrEqual(Object value) {
        this.value = value;
        this.op = Operator.LT_EQ;
        return this;
    }

    /**
     * Generates an equals constraint like {@code field &gt; value}.
     *
     * @param value the value to compare against
     * @return the constraint itself
     */
    public FieldOperator greaterThan(Object value) {
        this.value = value;
        this.op = Operator.GT;
        return this;
    }

    /**
     * Generates an equals constraint like {@code field &gt;= value}.
     *
     * @param value the value to compare against
     * @return the constraint itself
     */
    public FieldOperator greaterOrEqual(Object value) {
        this.value = value;
        this.op = Operator.GT_EQ;
        return this;
    }

    /**
     * Generates an equals constraint like {@code field &lt;&gt; value}.
     *
     * @param value the value to compare against
     * @return the constraint itself
     */
    public FieldOperator notEqual(Object value) {
        this.value = value;
        this.op = Operator.NE;
        return this;
    }

    /**
     * Permits to skip this constraint if the filter value is <tt>null</tt>.
     *
     * @return the constraint itself
     */
    public FieldOperator ignoreNull() {
        this.ignoreNull = true;
        return this;
    }

    /**
     * Also makes the constraint evaluate to <tt>true</tt>, if the field being addressed is <tt>null</tt>.
     *
     * @return the constraint itself
     */
    public FieldOperator orNull() {
        this.orNull = true;
        return this;
    }

    @Override
    public boolean addsConstraint() {
        return !ignoreNull || value != null;
    }

    @Override
    public void appendSQL(SmartQuery.Compiler compiler) {
        if (op == null) {
            throw new IllegalStateException("operator not set");
        }
        if (!addsConstraint()) {
            return;
        }

        String columnName = compiler.translateColumnName(field);
        if (value == null) {
            appendNullConstraint(compiler, columnName);
        } else {
            appendConstraint(compiler, columnName);
        }
    }

    protected void appendConstraint(SmartQuery.Compiler compiler, String columnName) {
        if (orNull) {
            compiler.getWHEREBuilder()
                    .append("(")
                    .append(columnName)
                    .append(op)
                    .append(" ? OR ")
                    .append(columnName)
                    .append(" IS NULL)");
        } else {
            compiler.getWHEREBuilder().append(columnName).append(op).append(" ?");
        }

        compiler.addParameter(Databases.convertValue(value));
    }

    protected void appendNullConstraint(SmartQuery.Compiler compiler, String columnName) {
        if (op == Operator.EQ) {
            compiler.getWHEREBuilder().append(columnName).append(" IS NULL");
        } else if (op == Operator.NE) {
            if (orNull) {
                // x IS NOT NULL OR x IS NULL === true
                // This constraint is pointless but as we might be in an OR, we still need to
                // output a constraint that is always true. As there is no boolean literal in
                // SQL, we use this brilliant expression:
                compiler.getWHEREBuilder().append("1=1");
            } else {
                compiler.getWHEREBuilder().append(columnName).append(" IS NOT NULL");
            }
        }
    }
}
