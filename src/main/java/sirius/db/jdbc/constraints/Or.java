/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.constraints;

import java.util.List;

/**
 * Represents a set of inner constraints combined with <tt>OR</tt>.
 */
class Or extends CombinedConstraint {

    protected Or(List<SQLConstraint> inner) {
        super(inner);
    }

    @Override
    protected String getCombiner() {
        return " OR ";
    }
}
