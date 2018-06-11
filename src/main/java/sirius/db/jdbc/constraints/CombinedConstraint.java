/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.constraints;

import com.google.common.collect.Lists;
import sirius.db.jdbc.Constraint;
import sirius.db.jdbc.SmartQuery;
import sirius.kernel.commons.Monoflop;

import java.util.Arrays;
import java.util.List;

/**
 * Combines a list of constraints into a single constraint using a logical operator.
 */
abstract class CombinedConstraint extends Constraint {

    protected List<Constraint> inner = Lists.newArrayList();

    /**
     * Creates a new combination for the given list of constraints.
     *
     * @param inner the constraints to combine
     */
    protected CombinedConstraint(List<Constraint> inner) {
        this.inner = inner;
    }

    /**
     * Creates a new combination for the given array of constraints.
     *
     * @param inner the constraints to combine
     */
    protected CombinedConstraint(Constraint... inner) {
        this.inner = Arrays.asList(inner);
    }

    @Override
    public boolean addsConstraint() {
        for (Constraint c : inner) {
            if (c.addsConstraint()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void appendSQL(SmartQuery.Compiler compiler) {
        if (addsConstraint()) {
            Monoflop mf = Monoflop.create();
            compiler.getWHEREBuilder().append("(");
            inner.stream().filter(Constraint::addsConstraint).forEach(c -> {
                if (mf.successiveCall()) {
                    compiler.getWHEREBuilder().append(getCombiner());
                }
                c.appendSQL(compiler);
            });
            compiler.getWHEREBuilder().append(")");
        }
    }

    /**
     * Returns the logical operator used to combine the constraints.
     *
     * @return the logical operator used to combine the constraints.
     */
    protected abstract String getCombiner();
}
