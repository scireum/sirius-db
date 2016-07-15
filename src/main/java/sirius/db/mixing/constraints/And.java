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
 * Represents an AND operation as {@link Constraint}.
 */
public class And extends CombinedConstraint {

    /**
     * Combines the given constraints using an AND operator.
     *
     * @param inner the list of constraints which all have to be fullfilled.
     * @return a constraint representing all the given constraints combined using AND
     */
    public static Constraint of(List<Constraint> inner) {
        if (inner.size() == 1) {
            return inner.get(0);
        }

        return new And(inner);
    }

    /**
     * Combines the given constraints using an AND operator.
     *
     * @param inner the array of constraints which all have to be fullfilled.
     * @return a constraint representing all the given constraints combined using AND
     */
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
