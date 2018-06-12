/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.kernel.commons.Value;

/**
 * Represents a context info which can be supplied for {@link BaseMapper#find(Class, Object, ContextInfo...)}.
 */
public class ContextInfo {

    private String key;
    private Value value;

    /**
     * Creates a new info object for the given key and value.
     *
     * @param key   the key used to pass in
     * @param value the value to pass in
     */
    public ContextInfo(String key, Value value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Returns the key of this info.
     *
     * @return the key of this info
     */
    public String getKey() {
        return key;
    }

    /**
     * Returns the value of this info.
     *
     * @return the value of this info
     */
    public Value getValue() {
        return value;
    }
}
