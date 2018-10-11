/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.types;

/**
 * Provides a map of <tt>String</tt> to <tt>Boolean</tt> as property value.
 */
public class StringBooleanMap extends SafeMap<String, Boolean> {

    @Override
    protected boolean valueNeedsCopy() {
        return false;
    }

    @Override
    protected Boolean copyValue(Boolean value) {
        return value;
    }
}
