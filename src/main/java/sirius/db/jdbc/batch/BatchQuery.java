/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.batch;

import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.jdbc.SQLEntity;
import sirius.db.jdbc.OMA;
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

public abstract class BatchQuery<E extends SQLEntity> {

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

    protected BatchQuery(BatchContext context, Class<E> type, String[] mappings) {
        this.context = context;
        this.type = type;
        this.mappings = mappings;
    }

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

    public void commit() {
        tryCommit(true);
    }

    protected void addBatch() throws SQLException {
        prepareStmt().addBatch();
        batchBacklog++;
        if (batchBacklog > batchBacklogLimit) {
            commit();
        }
    }

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

    protected PreparedStatement prepareStmt() throws SQLException {
        if (stmt == null) {
            buildSQL();
        }

        return stmt;
    }

    protected abstract void buildSQL() throws SQLException;

    protected EntityDescriptor getDescriptor() {
        if (descriptor == null) {
            descriptor = mixing.getDescriptor(type);
        }
        return descriptor;
    }

    protected List<Property> getProperties() {
        if (properties == null) {
            EntityDescriptor ed = getDescriptor();
            properties = Arrays.stream(mappings).map(ed::getProperty).collect(Collectors.toList());
        }

        return properties;
    }

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
}
