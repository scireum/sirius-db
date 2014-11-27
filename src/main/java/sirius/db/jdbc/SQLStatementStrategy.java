/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import com.google.common.collect.Lists;
import sirius.kernel.commons.Tuple;

import java.sql.*;
import java.util.List;

/**
 * Adapter to generate SQL statements via the {@link StatementCompiler}
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2013/11
 */
class SQLStatementStrategy implements StatementStrategy {

    private PreparedStatement stmt;
    private List<Tuple<Integer, Object>> parameters = Lists.newArrayList();
    private Connection c;
    private StringBuilder sb;
    private boolean mysql;
    private boolean retrieveGeneratedKeys;

    protected SQLStatementStrategy(Connection c, boolean mysql) {
        this.c = c;
        this.mysql = mysql;
        this.sb = new StringBuilder();
    }

    @Override
    public void appendString(String queryPart) {
        sb.append(queryPart);
    }

    @Override
    public void set(int idx, Object value) throws SQLException {
        parameters.add(Tuple.create(idx, value));
    }

    @Override
    public String getParameterName() {
        return "?";
    }

    @Override
    public String getQueryString() {
        return sb.toString();
    }

    public PreparedStatement getStmt() throws SQLException {
        if (stmt == null) {
            if (retrieveGeneratedKeys) {
                stmt = c.prepareStatement(sb.toString(), Statement.RETURN_GENERATED_KEYS);
            } else {
                stmt = c.prepareStatement(sb.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            }
            if (mysql) {
                // Switch MySQL driver into streaming mode...
                stmt.setFetchSize(Integer.MIN_VALUE);
            }
            for (Tuple<Integer, Object> t : parameters) {
                stmt.setObject(t.getFirst(), t.getSecond());
            }

        }
        return stmt;
    }

    public boolean isRetrieveGeneratedKeys() {
        return retrieveGeneratedKeys;
    }

    public void setRetrieveGeneratedKeys(boolean retrieveGeneratedKeys) {
        this.retrieveGeneratedKeys = retrieveGeneratedKeys;
    }
}
