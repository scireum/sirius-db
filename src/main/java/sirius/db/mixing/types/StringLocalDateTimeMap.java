/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.types;

import sirius.db.mixing.types.SafeMap;

import java.time.LocalDateTime;

/**
 * Represents map of <tt>String</tt> pointing to <tt>LocalDateTime</tt> values.
 */
public class StringLocalDateTimeMap extends SafeMap<String, LocalDateTime> {

    @Override
    protected boolean valueNeedsCopy() {
        return false;
    }

    @Override
    protected LocalDateTime copyValue(LocalDateTime value) {
        return value;
    }
}
