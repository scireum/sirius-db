/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Provides a map of string to list of strings as property value.
 */
public class StringListMap extends SafeMap<String, List<String>> {

    /**
     * Returns the list associated with the given key.
     *
     * @param key the key to lookup
     * @return the list stored for the given key or an empty list, if none is present
     */
    public List<String> getList(String key) {
        return get(key).orElse(Collections.emptyList());
    }

    /**
     * Adds the given value to the list of the given key.
     *
     * @param key   the key used to determine the target list
     * @param value the value to add
     * @return the map itself for fluent method calls
     */
    public StringListMap add(String key, String value) {
        modify().computeIfAbsent(key, k -> new ArrayList<>()).add(value);
        return this;
    }

    /**
     * Adds the given value to the list of the given key.
     * <p>
     * Note that the list is treated like a set, therefore, if the value is already in the list, it won't be added again.
     *
     * @param key   the key used to determine the target list
     * @param value the value to add
     * @return the map itself for fluent method calls
     */
    public StringListMap addToSet(String key, String value) {
        List<String> values = modify().computeIfAbsent(key, k -> new ArrayList<>());
        if (!values.contains(value)) {
            values.add(value);
        }

        return this;
    }

    /**
     * Removes the given value from the given list.
     *
     * @param key   the key used to determine the list to remove the value from
     * @param value the value to remove
     * @return the map itself for fluent method calls
     */
    public StringListMap remove(String key, String value) {
        get(key).ifPresent(list -> list.remove(value));
        return this;
    }

    /**
     * Determines if the list stored for <tt>key</tt> contains the given <tt>value</tt>.
     *
     * @param key   the key used to determine the list to check
     * @param value the value to search for
     * @return <tt>true</tt> if the value is in the expected list, <tt>false</tt> if it isn't or if there is no list
     * at all
     */
    public boolean contains(String key, String value) {
        return get(key).map(list -> list.contains(value)).orElse(false);
    }

    /**
     * Clears the list for the given key.
     *
     * @param key the key used to determine which list to clear
     * @return the map itself for fluent method calls
     */
    public StringListMap clear(String key) {
        get(key).ifPresent(List::clear);
        return this;
    }

    @Override
    protected boolean valueNeedsCopy() {
        return true;
    }

    @Override
    protected List<String> copyValue(List<String> value) {
        return new ArrayList<>(value);
    }
}
