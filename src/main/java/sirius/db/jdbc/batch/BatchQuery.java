/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.batch;

import sirius.db.jdbc.OMA;
import sirius.db.jdbc.SQLEntity;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.kernel.commons.Monoflop;
import sirius.kernel.commons.Watch;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Average;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;
import sirius.kernel.nls.NLS;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Provides an abstract wrapper around a {@link PreparedStatement} to be used within a {@link BatchContext}.
 *
 * @param <E> the type of entities being processed by this query.
 */
public abstract class BatchQuery<E extends SQLEntity> {

    /**
     * Defines the default batch size used by a query.
     */
    public static final int MAX_BATCH_BACKLOG = 250;

    protected BatchContext context;
    protected PreparedStatement stmt;
    protected int batchBacklog;
    protected int batchBacklogLimit = MAX_BATCH_BACKLOG;
    protected Class<E> type;
    protected final String[] mappings;
    protected List<Property> properties;
    protected EntityDescriptor descriptor;
    protected String query;
    protected Average avarage = new Average();

    @Part
    protected static OMA oma;

    @Part
    protected static Mixing mixing;

    /**
     * Creates a new instance for the given context, type and mappings.
     *
     * @param context  the batch context to participate in
     * @param type     the type of entities being processed
     * @param mappings the mappings to process
     */
    protected BatchQuery(BatchContext context, Class<E> type, String[] mappings) {
        this.context = context;
        this.type = type;
        this.mappings = mappings;
    }

    /**
     * Specifies a custom batch size for this query.
     *
     * @param maxBacklog the max. batch size to process
     */
    public void withCustomBatchLimit(int maxBacklog) {
        this.batchBacklogLimit = maxBacklog;
    }

    protected void tryCommit(boolean cascade) {
        if (stmt == null) {
            return;
        }

        if (batchBacklog > 0) {
            try {
                Watch w = Watch.start();
                stmt.executeBatch();
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
        prepareStmt().addBatch();
        batchBacklog++;
        if (batchBacklog > batchBacklogLimit) {
            commit();
        }
    }

    /**
     * Prepares a new statement for the given sql and options.
     *
     * @param sql                 the statement to prepare
     * @param returnGeneratedKeys determines if generated key should be returned or not
     * @throws SQLException in case of a database error
     */
    protected void createStmt(String sql, boolean returnGeneratedKeys) throws SQLException {
        if (stmt != null) {
            throw new IllegalStateException("A statement has already been prepared!");
        }

        this.query = sql;

        stmt = returnGeneratedKeys ?
               context.getConnection(getDescriptor().getRealm())
                      .prepareStatement(sql, Statement.RETURN_GENERATED_KEYS) :
               context.getConnection(getDescriptor().getRealm())
                      .prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    /**
     * Prepares a new statement if not done already.
     *
     * @return the prepared statment
     * @throws SQLException in case of a database error
     */
    protected PreparedStatement prepareStmt() throws SQLException {
        if (stmt == null) {
            buildSQL();
        }

        return stmt;
    }

    /**
     * Determines the SQL statement to prepare in {@link #prepareStmt()}.
     *
     * @throws SQLException in case of a database error
     */
    protected abstract void buildSQL() throws SQLException;

    /**
     * Determines the descriptor for the our entity type.
     *
     * @return the descriptor for the type of entities this query can process
     */
    protected EntityDescriptor getDescriptor() {
        if (descriptor == null) {
            descriptor = mixing.getDescriptor(type);
        }
        return descriptor;
    }

    /**
     * Transforms the mappings into a list of properties.
     *
     * @return the list of properties defined by the given list of mappings
     */
    protected List<Property> getProperties() {
        if (properties == null) {
            EntityDescriptor ed = getDescriptor();
            properties = Arrays.stream(mappings).map(ed::getProperty).collect(Collectors.toList());
        }

        return properties;
    }

    /**
     * Closes the query by executing the last batch and releasing all resources.
     */
    public void close() {
        if (stmt == null) {
            return;
        }

        try {
            commit();
        } catch (HandledException ex) {
            Exceptions.ignore(ex);
        }

        safeClose();
        context.unregister(this);
    }

    /**
     * Releases all resources with graceful error handling
     */
    protected void safeClose() {
        try {
            if (stmt != null) {
                stmt.close();
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
        if (descriptor != null) {
            sb.append(descriptor.getRelationName());
        } else {
            sb.append(" ?");
        }

        sb.append(" [");
        sb.append(stmt != null ? "open" : "closed");
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

        if (query != null) {
            sb.append(query);
        } else {
            sb.append(" no query available yet");
        }

        return sb.toString();
    }

    protected void buildWhere(StringBuilder sql, boolean addVersionConstraint) {
        sql.append(" WHERE ");
        Monoflop mf = Monoflop.create();
        for (Property p : getProperties()) {
            if (mf.successiveCall()) {
                sql.append(" AND ");
            }
            sql.append(p.getPropertyName());
            sql.append(" <=> ?");
        }

        if (!addVersionConstraint) {
            return;
        }

        if (descriptor.isVersioned()) {
            if (mf.successiveCall()) {
                sql.append(" AND ");
            }
            sql.append(BaseMapper.VERSION);
            sql.append(" = ?");
        }
    }
}
