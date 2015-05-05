/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing;

/**
 * Created by aha on 27.04.15.
 */
public abstract class Constraint {

    public abstract boolean addsConstraint();

    public abstract void appendSQL(SmartQuery.Compiler compiler);

    @Override
    public String toString() {
        if (!addsConstraint()) {
            return "[inactive constraint]";
        }
        SmartQuery.Compiler c = new SmartQuery.Compiler(null);
        appendSQL(c);
        return c.toString();
    }
}
