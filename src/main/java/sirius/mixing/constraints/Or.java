/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.constraints;

import sirius.mixing.Constraint;

import java.util.List;

/**
 * Created by aha on 29.04.15.
 */
public class Or extends CombinedConstraint {

    public static Constraint of(List<Constraint> inner) {
        if (inner.size() == 1) {
            return inner.get(0);
        }
        return new Or(inner);
    }

    public static Constraint of(Constraint... inner) {
        if (inner.length == 1) {
            return inner[0];
        }

        return new Or(inner);
    }

    protected Or(List<Constraint> inner) {
        super(inner);
    }

    protected Or(Constraint... inner) {
        super(inner);
    }

    protected String getCombiner() {
        return " OR ";
    }
}
