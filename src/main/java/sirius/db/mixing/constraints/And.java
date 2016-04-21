/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.constraints;

import sirius.db.mixing.Constraint;

import java.util.List;

/**
 * Created by aha on 29.04.15.
 */
public class And extends CombinedConstraint {

    public static Constraint of(List<Constraint> inner) {
        if (inner.size() == 1) {
            return inner.get(0);
        }

        return new And(inner);
    }

    public static Constraint of(Constraint... inner) {
        if (inner.length == 1) {
            return inner[0];
        }

        return new And(inner);
    }

    protected And(List<Constraint> inner) {
        super(inner);
    }

    protected And(Constraint... inner) {
        super(inner);
    }

    @Override
    protected String getCombiner() {
        return " AND ";
    }
}
