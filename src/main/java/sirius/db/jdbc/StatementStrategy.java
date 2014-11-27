/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import java.sql.SQLException;

/**
 * Defines a dialect used by {@link StatementCompiler} to parse and compile a query.
 * <p>
 * The default implementation is {@link SQLStatementStrategy} which generates a {@link java.sql.PreparedStatement}
 * for SQL queries
 * </p>
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2013/11
 */
public interface StatementStrategy {
    /**
     * Appends the qiven string to the output
     *
     * @param queryPart the string to add to the output
     */
    void appendString(String queryPart);

    /**
     * Sets the nth parameter
     *
     * @param idx   the index of the parameter to set
     * @param value the value of the parameter to set
     * @throws SQLException in case of a database error
     */
    void set(int idx, Object value) throws SQLException;

    /**
     * Returns the template string used for parameters
     *
     * @return the string used to signal a template parameter
     */
    String getParameterName();

    /**
     * Returns the finalized query as string.
     *
     * @return the generated query as string
     */
    String getQueryString();
}
