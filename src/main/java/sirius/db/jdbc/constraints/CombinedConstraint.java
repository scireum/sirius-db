/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.constraints;

import sirius.db.jdbc.SmartQuery;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Monoflop;

import java.util.List;
import java.util.Objects;

/**
 * Base class for combining constraints.
 */
abstract class CombinedConstraint extends SQLConstraint {

    protected List<SQLConstraint> inner;

    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Explain("This is only used internally so copying / wrapping etc. aren't worth the overhead.")
    protected CombinedConstraint(List<SQLConstraint> inner) {
        this.inner = inner;
    }

    @Override
    public void appendSQL(SmartQuery.Compiler compiler) {
        Monoflop mf = Monoflop.create();
        compiler.getWHEREBuilder().append("(");
        inner.stream().filter(Objects::nonNull).forEach(c -> {
            if (mf.successiveCall()) {
                compiler.getWHEREBuilder().append(getCombiner());
            }
            c.appendSQL(compiler);
        });
        compiler.getWHEREBuilder().append(")");
    }

    @Override
    public void asString(StringBuilder builder) {
        Monoflop mf = Monoflop.create();
        builder.append("(");
        inner.stream().filter(Objects::nonNull).forEach(c -> {
            if (mf.successiveCall()) {
                builder.append(getCombiner());
            }
            c.asString(builder);
        });
        builder.append(")");
    }

    /**
     * Returns the operator used to combine the inner constraints
     *
     * @return the operator to use
     */
    protected abstract String getCombiner();
}
