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
 * Represents a constraint which ensures that a field is not filled / null.
 */
class NotFilled extends SQLConstraint {

    private Mapping field;

    protected NotFilled(Mapping field) {
        this.field = field;
    }

    @Override
    public void appendSQL(SmartQuery.Compiler compiler) {
        String columnName = compiler.translateColumnName(field);
        compiler.getWHEREBuilder().append(columnName).append(" IS NULL");
    }

    @Override
    public void asString(StringBuilder builder) {
        builder.append(field.toString()).append(" IS NULL");
    }
}
