/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.constraints;

import com.google.common.collect.Lists;
import sirius.kernel.commons.Monoflop;
import sirius.mixing.Constraint;
import sirius.mixing.SmartQuery;

import java.util.Arrays;
import java.util.List;

/**
 * Created by aha on 29.04.15.
 */
public abstract class CombinedConstraint extends Constraint {

    protected List<Constraint> inner = Lists.newArrayList();

    protected CombinedConstraint(List<Constraint> inner) {
        this.inner = inner;
    }

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

    protected abstract String getCombiner();
}
