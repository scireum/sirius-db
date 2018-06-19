/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.types;

/**
 * Provides a map of <tt>String</tt> to <tt>int</tt> as property value.
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
