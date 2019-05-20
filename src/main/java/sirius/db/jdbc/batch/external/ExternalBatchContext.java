/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.batch.external;

import sirius.db.jdbc.Database;
import sirius.db.jdbc.OMA;
import sirius.kernel.async.Operation;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Bundles the preparation and execution of a bunch of {@link ExternalBatchQuery batch queries}.
 * <p>
 * In contrast to {@link sirius.db.jdbc.batch.BatchContext} which can only be used with the databases known
 * and managed by {@link sirius.db.mixing.Mixing}, this can be used with any JDBC datasource available
 * as {@link sirius.db.jdbc.Database}.
 */
@NotThreadSafe
public class ExternalBatchContext implements Closeable {

    private Database database;
    private Connection connection;
    private List<ExternalBatchQuery> queries = new ArrayList<>();
    private Operation op;

    /**
     * Creates a new context with the given debugging description and the expected runtime.
     *
     * @param description      a provider for a description used for debugging purposes
     * @param expectedDuration the expected duration of the whole batch operation
     * @param database         the database against which the queries are executed
     */
    public ExternalBatchContext(Supplier<String> description, Duration expectedDuration, Database database) {
        this.database = database;
        this.op = new Operation(description, expectedDuration);
    }

    /**
     * Creates a new batch query which essentially wraps the {@link java.sql.PreparedStatement} created for the given SQL.
     *
     * @param sql the query to prepare
     * @return the prepared statement wrapped as batch query
     * @throws SQLException in case of a database error
     */
    public ExternalBatchQuery prepare(String sql) throws SQLException {
        ExternalBatchQuery qry = new ExternalBatchQuery(this, sql, getConnection());
        queries.add(qry);
        return qry;
    }

    protected Connection getConnection() throws SQLException {
        if (connection == null) {
            connection = database.getConnection();
        }

        return connection;
    }

    protected void safeClose() {
        for (ExternalBatchQuery query : queries) {
            try {
                query.tryCommit(false);
            } catch (HandledException e) {
                Exceptions.ignore(e);
            } catch (Exception e) {
                Exceptions.handle(OMA.LOG, e);
            }

            query.safeClose();
        }
        queries.clear();
        safeCloseConnection();
    }

    private void safeCloseConnection() {
        try {
            if (connection != null) {
                connection.close();
                connection = null;
            }
        } catch (SQLException e) {
            Exceptions.handle()
                      .to(OMA.LOG)
                      .error(e)
                      .withSystemErrorMessage("An exception occured while closing a database connection: %s (%s)")
                      .handle();
        }
    }

    @Override
    public void close() throws IOException {
        safeClose();
        op.close();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("External Batch Context\n");
        sb.append("----------------------------------------\n");
        for (ExternalBatchQuery query : queries) {
            sb.append(query).append("\n");
        }
        sb.append("----------------------------------------\n");

        return sb.toString();
    }
}
