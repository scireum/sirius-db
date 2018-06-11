/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.constraints;

import sirius.db.jdbc.Constraint;
import sirius.db.jdbc.SmartQuery;

/**
 * Inverts the given constraint.
 */
public class Not extends Constraint {

    private Constraint inner;

    private Not(Constraint inner) {
        this.inner = inner;
    }

    /**
     * Creates an inverse constraint of the given one.
     * <p>
     * If multiple constraints are given, the are combined with AND so that they all have to be <tt>false</tt>  for
     * this
     * constraint to be come <tt>true</tt>.
     *
     * @param inner the constraints to invert
     * @return a constraint which represents the inverse of the given constraint
     */
    public static Not of(Constraint... inner) {
        if (inner.length == 1) {
            return new Not(inner[0]);
        } else if (inner.length == 0) {
            return new Not(null);
        } else {
            return new Not(And.of(inner));
        }
    }

    @Override
    public boolean addsConstraint() {
        return inner != null && inner.addsConstraint();
    }

    @Override
    public void appendSQL(SmartQuery.Compiler compiler) {
        compiler.getWHEREBuilder().append("NOT(");
        inner.appendSQL(compiler);
        compiler.getWHEREBuilder().append(")");
    }
}
