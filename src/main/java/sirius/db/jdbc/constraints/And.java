/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.constraints;

import java.util.List;

/**
 * Represents a set of inner constraints combined with <tt>AND</tt>.
 */
class And extends CombinedConstraint {

    protected And(List<SQLConstraint> inner) {
        super(inner);
    }

    @Override
    protected String getCombiner() {
        return " AND ";
    }
}
