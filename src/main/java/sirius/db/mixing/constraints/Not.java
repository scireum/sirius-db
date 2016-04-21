/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.constraints;

import sirius.db.mixing.Constraint;
import sirius.db.mixing.SmartQuery;

/**
 * Created by aha on 29.04.15.
 */
public class Not extends Constraint {

    private Constraint inner;

    private Not(Constraint inner) {
        this.inner = inner;
    }

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
