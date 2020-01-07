/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.types;

/**
 * Provides a simple list of strings as property value.
 */
public class StringList extends SafeList<String> {

    private boolean autoparse;

    /**
     * Enables auto-parsing when performing a load from a form input or import.
     * <p>
     * Auto-parsing will expect a single string value and split at each occurrence of a ",".
     *
     * @return the list itself for fluent method calls
     */
    public StringList enableAutoparse() {
        this.autoparse = true;
        return this;
    }

    /**
     * Determines if auto-parsing is enabled.
     *
     * @return <tt>true</tt> if auto-parse is enabled, <tt>false</tt> otherwise
     */
    public boolean isAutoparse() {
        return autoparse;
    }

    @Override
    protected boolean valueNeedsCopy() {
        return false;
    }

    @Override
    protected String copyValue(String value) {
        return value;
    }
}
