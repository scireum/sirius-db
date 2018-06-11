/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

public class StringMap extends SafeMap<String, String> {

    @Override
    protected boolean valueNeedsCopy() {
        return false;
    }

    @Override
    protected String copyValue(String value) {
        return value;
    }
}
