/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.Mapping;

/**
 * Enumerates the operators supported by {@link GeneratedStatement#where(Mapping, Operator, Object)}
 * or {@link sirius.db.jdbc.batch.BatchQuery}.
 */
public enum Operator {
    LT("<"), LT_EQ("<="), EQ("="), GT_EQ(">="), GT(">"), NE("<>");

    private final String operation;

    Operator(String operation) {
        this.operation = operation;
    }

    @Override
    public String toString() {
        return operation;
    }
}
