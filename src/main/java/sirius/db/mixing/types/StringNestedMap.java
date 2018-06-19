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
 * Represents map of <tt>String</tt> pointing to {@link Nested} values.
 *
 * @param <N> specifies the type of nested objects within this map
 */
public class StringNestedMap<N extends Nested> extends SafeMap<String, N> {

    private Class<N> nestedType;

    /**
     * Creates a new instance capable of storing the given nested type.
     *
     * @param nestedType the type of items being stored
     */
    public StringNestedMap(Class<N> nestedType) {
        this.nestedType = nestedType;
    }

    /**
     * Returns the type of items stored as values in this map.
     *
     * @return the type of values in this map
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
