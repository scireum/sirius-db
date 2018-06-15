/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.types;

import sirius.db.mixing.types.SafeMap;

/**
 * Represents map of <tt>String</tt> pointing to <tt>int</tt> values.
 */
public class StringIntMap extends SafeMap<String, Integer> {

    @Override
    protected boolean valueNeedsCopy() {
        return false;
    }

    @Override
    protected Integer copyValue(Integer value) {
        return value;
    }
}
