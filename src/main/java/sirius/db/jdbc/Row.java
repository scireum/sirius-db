/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Value;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A small wrapper class to represent a result row.
 */
public class Row {

    protected Map<String, Tuple<String, Object>> fields = new LinkedHashMap<>();

    /**
     * Returns all stored fields as map.
     *
     * @return the underlying map, containing all fields and values. Modifying this map will modify the row
     */
    @Nonnull
    public Collection<Tuple<String, Object>> getFieldsList() {
        return fields.values();
    }

    /**
     * Determines if a value for a given key is present.
     *
     * @param key the key to check for
     * @return <tt>true</tt> if a value for the given key is present (even if it is <tt>null</tt>),
     * <tt>false</tt> otherwise
     */
    public boolean hasValue(@Nonnull String key) {
        return fields.containsKey(key.toUpperCase());
    }

    /**
     * Returns the value associated with the given key.
     *
     * @param key the name of the field to retrieve
     * @return the value associated with the given key wrapped as {@link sirius.kernel.commons.Value}
     * @throws java.lang.IllegalArgumentException if an unknown column key is requested
     * @see #tryGetValue(Object)
     */
    @Nonnull
    public Value getValue(@Nonnull Object key) {
        String upperCaseKey = key.toString().toUpperCase();
        if (!fields.containsKey(upperCaseKey)) {
            throw new IllegalArgumentException(Strings.apply("Unknown column: %s in %s", upperCaseKey, this));
        }
        return Value.of(fields.get(upperCaseKey).getSecond());
    }

    /**
     * Returns the value associated with the given key, returns an empty <tt>Value</tt> if the row doesn't contain the
     * value at all.
     *
     * @param key the name of the field to retrieve
     * @return the value associated with the given key wrapped as {@link sirius.kernel.commons.Value}
     * @see #getValue(Object)
     */
    @Nonnull
    public Value tryGetValue(@Nonnull Object key) {
        String upperCaseKey = key.toString().toUpperCase();

        Tuple<String, Object> value = fields.get(upperCaseKey);
        if (value == null) {
            return Value.EMPTY;
        }

        return Value.of(value.getSecond());
    }

    /**
     * Returns a sub list which was stored for a given key.
     * <p>
     * It is assumed that the object associated with the given key is a {@code List&lt;Row&gt;}.
     *
     * @param key the name of the field to retrieve
     * @return the list associated with the given key
     * @throws ClassCastException if the stored value isn't a list. <tt>null</tt> is handled gracefully
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public List<Row> getSublist(@Nonnull Object key) {
        return (List<Row>) getValue(key).get(Collections.emptyList());
    }

    /**
     * Stores a value for the given key.
     * <p>
     * Can be used to add computed values for further processing.
     *
     * @param key   the key to bind the value to
     * @param value the value to be stored
     */
    public void setValue(@Nonnull String key, Object value) {
        fields.put(key.toUpperCase(), Tuple.create(key, value));
    }

    @Override
    public String toString() {
        return "Row [" + fields + "]";
    }
}
