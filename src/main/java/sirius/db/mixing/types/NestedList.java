/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.types;

import sirius.db.mixing.Nested;

public class NestedList<N extends Nested> extends SafeList<N> {

    private Class<N> nestedType;

    public NestedList(Class<N> nestedType) {
        this.nestedType = nestedType;
    }

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
