/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

/**
 * Represents a constraint which can be added to the WHERE part of a SQL query.
 * <p>
 * Used by {@link SmartQuery} to generate filters for a query.
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2015/05
 */
public abstract class Constraint {

    /**
     * Determines if a constraint is actually generated or if this can be ignored.
     * <p>
     * Some constraints ignore <tt>null</tt> values and therefore will not generate a constraint when its filter value
     * is <tt>null</tt>.
     *
     * @return <tt>true</tt> if a constraint will be added to the query, <tt>false</tt> if i can be ignored.
     */
    public abstract boolean addsConstraint();

    /**
     * Appends the generated SQL to the given query compiler.
     *
     * @param compiler the compiler used to generated the effective {@link java.sql.PreparedStatement}
     */
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
