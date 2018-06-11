/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.batch;

import sirius.db.jdbc.BaseSQLQuery;
import sirius.db.jdbc.Databases;
import sirius.db.jdbc.Row;
import sirius.db.jdbc.SQLEntity;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CustomQuery extends BatchQuery<SQLEntity> {

    private final boolean fetchId;
    private final String sql;
    private final BatchSQLQuery sqlQuery;

    @SuppressWarnings("unchecked")
    protected CustomQuery(BatchContext context, Class<? extends SQLEntity> type, boolean fetchId, String sql) {
        super(context, (Class<SQLEntity>) type, null);

        this.fetchId = fetchId;
        this.sql = sql;
        this.sqlQuery = new BatchSQLQuery(this);
    }

    public void clearParameters() throws SQLException {
        PreparedStatement stmt = prepareStmt();
        stmt.clearParameters();
    }

    public void setParameter(int oneBasedIndex, Object value) throws SQLException {
        PreparedStatement stmt = prepareStmt();
        stmt.setObject(oneBasedIndex, Databases.convertValue(value));
    }

    @Nullable
    public Row executeUpdate() throws SQLException {
        prepareStmt().executeUpdate();
        if (fetchId) {
            return BaseSQLQuery.fetchGeneratedKeys(stmt);
        } else {
            return null;
        }
    }

    public void executeBatchUpdate() throws SQLException {
        addBatch();
    }

    public BatchSQLQuery query() {
        return sqlQuery;
    }

    @Override
    protected void buildSQL() throws SQLException {
        createStmt(sql, fetchId);
    }
}
