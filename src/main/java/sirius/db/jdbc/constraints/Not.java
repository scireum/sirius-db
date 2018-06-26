/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.constraints;

import sirius.db.jdbc.SmartQuery;

/**
 * Represents a constraint which inverts the inner constraint.
 */
class Not extends SQLConstraint {

    private SQLConstraint inner;

    protected Not(SQLConstraint inner) {
        this.inner = inner;
    }

    @Override
    public void appendSQL(SmartQuery.Compiler compiler) {
        compiler.getWHEREBuilder().append("NOT(");
        inner.appendSQL(compiler);
        compiler.getWHEREBuilder().append(")");
    }

    @Override
    public void asString(StringBuilder builder) {
        builder.append("NOT(");
        inner.asString(builder);
        builder.append(")");
    }
}
