/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.constraints;

import sirius.db.jdbc.SmartQuery;
import sirius.db.mixing.Mapping;

/**
 * Represents a simple field operator as constraint.
 */
class FieldOperator extends SQLConstraint {

    private Mapping field;
    private Object value;
    private String op;

    protected FieldOperator(Mapping field, String op, Object value) {
        this.field = field;
        this.op = op;
        this.value = value;
    }

    @Override
    public void appendSQL(SmartQuery.Compiler compiler) {
        String columnName = compiler.translateColumnName(field);
        compiler.getWHEREBuilder().append(columnName).append(op).append(" ?");
        compiler.addParameter(value);
    }

    @Override
    public void asString(StringBuilder builder) {
        builder.append(field.toString()).append(" ").append(op).append(" ").append(value);
    }
}
