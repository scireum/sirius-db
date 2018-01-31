/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.kernel.commons.Tuple;

import java.util.Map;

/**
 * Keeps track of the internal JOIN and column translation state of a {@link sirius.db.mixing.SmartQuery.Compiler}.
 * <p>
 * This is mainly used by constraints which generate internal JOINS like {@link sirius.db.mixing.constraints.Exists}.
 */
public class TranslationState {

    private final EntityDescriptor ed;
    private final String defaultAlias;
    private final StringBuilder joins;
    private final Map<String, Tuple<String, EntityDescriptor>> joinTable;

    protected TranslationState(EntityDescriptor ed,
                            String defaultAlias,
                            StringBuilder joins,
                            Map<String, Tuple<String, EntityDescriptor>> joinTable) {

        this.ed = ed;
        this.defaultAlias = defaultAlias;
        this.joins = joins;
        this.joinTable = joinTable;
    }

    protected EntityDescriptor getEd() {
        return ed;
    }

    protected String getDefaultAlias() {
        return defaultAlias;
    }

    protected StringBuilder getJoins() {
        return joins;
    }

    protected Map<String, Tuple<String, EntityDescriptor>> getJoinTable() {
        return joinTable;
    }
}
