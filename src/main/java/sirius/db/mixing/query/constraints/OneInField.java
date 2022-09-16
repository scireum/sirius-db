/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.query.constraints;

import sirius.db.mixing.Mapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a constraint which verifies that the given field contains one of the given values.
 *
 * @param <C> the effective type of constraint generated by this builder
 */
public class OneInField<C extends Constraint> {

    protected final Collection<?> values;
    protected FilterFactory<C> factory;
    protected final Mapping field;
    protected boolean orEmpty = false;
    protected boolean forceEmpty = false;

    protected OneInField(FilterFactory<C> factory, Mapping field, Collection<?> values) {
        this.factory = factory;
        this.field = field;
        if (values != null) {
            this.values = values.stream().filter(Objects::nonNull).toList();
        } else {
            this.values = Collections.emptyList();
        }
    }

    /**
     * Signals that this constraint is also fulfilled if the target field is empty.
     * <p>
     * This will convert this constraint into a filter.
     *
     * @return the constraint itself for fluent method calls
     */
    public OneInField<C> orEmpty() {
        orEmpty = true;
        return this;
    }

    /**
     * Signals that an empty input list is not ignored but enforces the target field to be empty.
     * <p>
     * This will convert this constraint into a filter.
     *
     * @return the constraint itself for fluent method calls
     */
    public OneInField<C> forceEmpty() {
        orEmpty = true;
        forceEmpty = true;
        return this;
    }

    /**
     * Generates the effective constraint.
     *
     * @return the constraint to be added to a {@link sirius.db.mixing.query.Query}.
     */
    public C build() {
        if (values.isEmpty()) {
            if (forceEmpty) {
                return factory.notFilled(field);
            }

            return null;
        }

        List<C> clauses = new ArrayList<>();
        for (Object value : values) {
            clauses.add(factory.eq(field, value));
        }

        if (orEmpty) {
            clauses.add(factory.notFilled(field));
        }

        return factory.effectiveOr(clauses);
    }
}
