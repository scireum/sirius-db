/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.query.constraints;

/**
 * Represents a constraint which can be applied to a {@link sirius.db.mixing.query.Query}.
 */
public abstract class Constraint {

    /**
     * Creates a string representation of this constraint.
     *
     * @param builder the target to write the string representation to
     */
    public abstract void asString(StringBuilder builder);

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        asString(builder);
        return builder.toString();
    }
}
