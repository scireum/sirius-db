/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.batch;

import sirius.db.jdbc.BaseSQLQuery;
import sirius.db.jdbc.Row;
import sirius.kernel.async.TaskContext;
import sirius.kernel.commons.Limit;
import sirius.kernel.commons.Watch;

import javax.annotation.Nullable;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Predicate;

/**
 * Represents an {@link BaseSQLQuery sql query} which can be used within a {@link BatchContext} by creating a {@link CustomQuery}.
 */
public class BatchSQLQuery extends BaseSQLQuery {

    protected CustomQuery query;

    protected BatchSQLQuery(CustomQuery customQuery) {
        this.query = customQuery;
    }

    @Override
    public void iterate(Predicate<Row> handler, @Nullable Limit limit) throws SQLException {
        Watch w = Watch.start();

        try (ResultSet rs = query.prepareStmt().executeQuery()) {
            query.average.addValue(w.elapsedMillis());
            TaskContext tc = TaskContext.get();
            processResultSet(handler, limit, rs, tc);
        }
    }

    @Override
    protected void writeBlobToParameter(String name, Blob blob) throws SQLException {
        throw new UnsupportedOperationException();
    }
}
