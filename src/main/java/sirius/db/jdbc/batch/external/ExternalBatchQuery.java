/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.batch.external;

import sirius.db.jdbc.Databases;
import sirius.db.jdbc.OMA;
import sirius.kernel.commons.Watch;
import sirius.kernel.health.Average;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;
import sirius.kernel.nls.NLS;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Provides an abstract wrapper around a {@link PreparedStatement} to be used within a {@link ExternalBatchContext}.
 */
public class ExternalBatchQuery {

    /**
     * Defines the default batch size used by a query.
     */
    public static final int MAX_BATCH_BACKLOG = 250;

    protected final PreparedStatement statement;
    protected int batchBacklog;
    protected int batchBacklogLimit = MAX_BATCH_BACKLOG;
    protected ExternalBatchContext context;
    protected String query;
    protected Average avarage = new Average();

    protected ExternalBatchQuery(ExternalBatchContext context, String query, Connection connection)
            throws SQLException {
        this.context = context;
        this.query = query;
        this.statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    /**
     * Specifies a custom batch size for this query.
     *
     * @param maxBacklog the max. batch size to process
     */
    public void withCustomBatchLimit(int maxBacklog) {
        this.batchBacklogLimit = maxBacklog;
    }

    /**
     * Resets all previously set parameters.
     *
     * @throws SQLException in case of a database error
     */
    public void clearParameters() throws SQLException {
        statement.clearParameters();
    }

    /**
     * Sets the given parameter to the given value.
     *
     * @param oneBasedIndex the one-based index of the parameter to set
     * @param value         the parameter value to set
     * @throws SQLException in case of a database error
     */
    public void setParameter(int oneBasedIndex, Object value) throws SQLException {
        statement.setObject(oneBasedIndex, Databases.convertValue(value));
    }

    protected void tryCommit(boolean cascade) {
        if (batchBacklog > 0) {
            try {
                Watch w = Watch.start();
                statement.executeBatch();
                avarage.addValues(batchBacklog, w.elapsedMillis());
                batchBacklog = 0;
            } catch (SQLException e) {
                if (cascade) {
                    context.safeClose();
                }
                throw Exceptions.handle()
                                .to(OMA.LOG)
                                .error(e)
                                .withSystemErrorMessage("An error occured while batch executing a statement: %s (%s)")
                                .handle();
            }
        }
    }

    /**
     * Forces a batch to be processed (independent of it size, as long as it isn't empty).
     */
    public void commit() {
        tryCommit(true);
    }

    /**
     * Adds the current parameter set as batch.
     *
     * @throws SQLException in case of a database error
     */
    protected void addBatch() throws SQLException {
        statement.addBatch();
        batchBacklog++;
        if (batchBacklog > batchBacklogLimit) {
            commit();
        }
    }

    /**
     * Closes the query by executing the last batch and releasing all resources.
     */
    public void close() {
        try {
            commit();
        } catch (HandledException ex) {
            Exceptions.ignore(ex);
        }

        safeClose();
    }

    /**
     * Releases all resources with graceful error handling
     */
    protected void safeClose() {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            Exceptions.handle()
                      .to(OMA.LOG)
                      .error(e)
                      .withSystemErrorMessage("An error occured while closing a prepared statement: %s (%s)")
                      .handle();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append(statement != null ? "open" : "closed");
        if (batchBacklog > 0) {
            sb.append("|Backlog: ");
            sb.append(batchBacklog);
        }
        if (avarage.getCount() > 0) {
            sb.append("|Executed: ");
            sb.append(avarage.getCount());
            sb.append("|Duration: ");
            sb.append(NLS.toUserString(avarage.getAvg()));
            sb.append(" ms");
        }
        sb.append("] ");
        sb.append(query);

        return sb.toString();
    }
}
