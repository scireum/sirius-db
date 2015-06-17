/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.constraints;

import sirius.kernel.commons.Amount;
import sirius.mixing.Column;
import sirius.mixing.Constraint;
import sirius.mixing.Entity;
import sirius.mixing.EntityRef;
import sirius.mixing.SmartQuery;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;

/**
 * Created by aha on 27.04.15.
 */
public class FieldOperator extends Constraint {

    public enum Operator {
        LT("<"), LT_EQ("<="), EQ("="), GT_EQ(">="), GT(">"), NE("<>");

        private String sql;

        Operator(String sql) {
            this.sql = sql;
        }

        @Override
        public String toString() {
            return sql;
        }
    }

    private Column field;
    private Object value;
    private Operator op;
    private boolean ignoreNull;

    private FieldOperator(Column field) {
        this.field = field;
    }

    public static FieldOperator on(Column field) {
        return new FieldOperator(field);
    }

    public FieldOperator equal(Object value) {
        this.value = value;
        this.op = Operator.EQ;
        return this;
    }

    public FieldOperator lessThan(Object value) {
        this.value = value;
        this.op = Operator.LT;
        return this;
    }

    public FieldOperator lessOrQual(Object value) {
        this.value = value;
        this.op = Operator.LT_EQ;
        return this;
    }

    public FieldOperator greaterThan(Object value) {
        this.value = value;
        this.op = Operator.GT;
        return this;
    }

    public FieldOperator greaterOrQual(Object value) {
        this.value = value;
        this.op = Operator.GT_EQ;
        return this;
    }

    public FieldOperator notEqual(Object value) {
        this.value = value;
        this.op = Operator.NE;
        return this;
    }

    public FieldOperator ignoreNull() {
        this.ignoreNull = true;
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
        if (addsConstraint()) {
            if (value == null) {
                if (op == Operator.EQ) {
                    compiler.getWHEREBuilder().append(compiler.translateColumnName(field)).append(" IS NULL");
                    return;
                } else if (op == Operator.NE) {
                    compiler.getWHEREBuilder().append(compiler.translateColumnName(field)).append(" IS NOT NULL");
                    return;
                }
            }
            compiler.getWHEREBuilder().append(compiler.translateColumnName(field)).append(op).append(" ?");
            compiler.addParameter(convertValue(value));
        }
    }

    private Object convertValue(Object value) {
        if (value == null) {
            return value;
        }
        if (value instanceof LocalDateTime) {
            return new Timestamp(((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
        } else if (value instanceof LocalDate) {
            return new Date(((LocalDate) value).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli());
        } else if (value instanceof LocalTime) {
            return new Time(((LocalTime) value).atDate(LocalDate.of(1970, 01, 01))
                                               .atZone(ZoneId.systemDefault())
                                               .toInstant()
                                               .toEpochMilli());
        } else if (value instanceof Amount) {
            return ((Amount) value).getAmount();
        } else if (value.getClass().isEnum()) {
            return ((Enum<?>) value).name();
        } else if (value instanceof EntityRef) {
            return ((EntityRef<?>) value).getId();
        } else if (value instanceof Entity) {
            return ((Entity) value).getId();
        }

        return value;
    }
}
