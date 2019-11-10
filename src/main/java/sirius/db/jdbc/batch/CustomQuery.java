/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.batch;

import sirius.db.jdbc.Databases;
import sirius.db.jdbc.Row;
import sirius.db.jdbc.SQLEntity;
import sirius.kernel.di.std.Part;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;

/**
 * Wraps a SQL statement as {@link BatchSQLQuery}.
 */
public class CustomQuery extends BatchQuery<SQLEntity> {

    private final boolean fetchId;
    private final String sql;
    private final BatchSQLQuery sqlQuery;

    @Part
    private static Databases dbs;

    @SuppressWarnings("unchecked")
    protected CustomQuery(BatchContext context, Class<? extends SQLEntity> type, boolean fetchId, String sql) {
        super(context, (Class<SQLEntity>) type, Collections.emptyList());

        this.fetchId = fetchId;
        this.sql = sql;
        this.sqlQuery = new BatchSQLQuery(this);
    }

    /**
     * Resets all previously set parameters.
     *
     * @throws SQLException in case of a database error
     */
    public void clearParameters() throws SQLException {
        PreparedStatement stmt = prepareStmt();
        stmt.clearParameters();
    }

    /**
     * Sets the given parameter to the given value.
     *
     * @param oneBasedIndex the one-based index of the parameter to set
     * @param value         the parameter value to set
     * @throws SQLException in case of a database error
     */
    public void setParameter(int oneBasedIndex, Object value) throws SQLException {
        PreparedStatement stmt = prepareStmt();
        stmt.setObject(oneBasedIndex, Databases.convertValue(value));
    }

    /**
     * Executes the prepared statement as update (or delete).
     *
     * @return a row containing the generated keys or <tt>null</tt>if fetching is disabled
     * @throws SQLException in case of a database error
     */
    @Nullable
    public Row executeUpdate() throws SQLException {
        prepareStmt().executeUpdate();
        prepareStmt().getConnection().commit();
        if (fetchId) {
            return dbs.fetchGeneratedKeys(stmt);
        } else {
            return null;
        }
    }

    /**
     * Executes the statement as batched update.
     *
     * @throws SQLException in case of a database error
     */
    public void executeBatchUpdate() throws SQLException {
        addBatch();
    }

    /**
     * Executes the statement as query.
     *
     * @return the statement as query
     */
    public BatchSQLQuery query() {
        return sqlQuery;
    }

    @Override
    protected void buildSQL() throws SQLException {
        createStmt(sql, fetchId);
    }
}
