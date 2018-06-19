/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.types;

import sirius.db.mixing.Nested;

/**
 * Represents a list of {@link Nested} objects.
 *
 * @param <N> the type of nested objects in this list
 */
public class NestedList<N extends Nested> extends SafeList<N> {

    private Class<N> nestedType;

    /**
     * Creates a new list for the given type.
     *
     * @param nestedType the type of objects stored in this list
     */
    public NestedList(Class<N> nestedType) {
        this.nestedType = nestedType;
    }

    /**
     * Returns the type of objects stored in this list.
     *
     * @return the type of objects stored in this list
     */
    public Class<N> getNestedType() {
        return nestedType;
    }

    @Override
    protected boolean valueNeedsCopy() {
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected N copyValue(N value) {
        return value == null ? null : (N) value.copy();
    }
}
