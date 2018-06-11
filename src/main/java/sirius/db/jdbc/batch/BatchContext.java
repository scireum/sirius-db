/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.batch;

import sirius.db.jdbc.SQLEntity;
import sirius.db.jdbc.OMA;
import sirius.db.mixing.Mapping;
import sirius.kernel.async.Operation;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class BatchContext implements Closeable {

    @Part
    private static OMA oma;

    private List<BatchQuery<?>> queries = new ArrayList<>();
    private Map<String, Connection> connectionsPerRealm = new HashMap<>();
    private Operation op;

    public BatchContext(Supplier<String> description, Duration expectedDuration) {
        this.op = new Operation(description, expectedDuration);
    }

    private <Q extends BatchQuery<?>> Q register(Q query) {
        this.queries.add(query);
        return query;
    }

    protected void unregister(BatchQuery<?> query) {
        this.queries.remove(query);
    }

    protected static String[] simplifyMappings(Mapping[] mappingsToCompare) {
        return Arrays.stream(mappingsToCompare).map(Mapping::toString).toArray(n -> new String[n]);
    }

    protected Connection getConnection(String realm) {
        return connectionsPerRealm.computeIfAbsent(realm, this::createConnection);
    }

    protected Connection createConnection(String realm) {
        try {
            return oma.getDatabase(realm).getConnection();
        } catch (SQLException e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("Failed to open a database connection for realm '%s': %s (%s)",
                                                    realm)
                            .handle();
        }
    }

    protected void safeClose() {
        for (BatchQuery<?> query : queries) {
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

        connectionsPerRealm.values().forEach(this::safeCloseConnection);
        connectionsPerRealm.clear();
    }

    private void safeCloseConnection(Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            Exceptions.handle()
                      .to(OMA.LOG)
                      .error(e)
                      .withSystemErrorMessage("An exception occured while closing a database connection: %s (%s)")
                      .handle();
        }
    }

    public <E extends SQLEntity> FindQuery<E> findQuery(Class<E> type, Mapping... mappingsToCompare) {
        return register(new FindQuery<>(this, type, simplifyMappings(mappingsToCompare)));
    }

    public FindQuery<?> autoFindQuery(String... mappingsToCompare) {
        return register(new FindQuery<>(this, null, mappingsToCompare));
    }

    public <E extends SQLEntity> InsertQuery<E> insertQuery(Class<E> type, boolean fetchId, Mapping... mappingsToInsert) {
        return register(new InsertQuery<>(this, type, fetchId, simplifyMappings(mappingsToInsert)));
    }

    public InsertQuery<?> autoInsertQuery(boolean fetchId, String... mappingsToInsert) {
        return register(new InsertQuery<>(this, null, fetchId, mappingsToInsert));
    }

    public <E extends SQLEntity> InsertQuery<E> insertQuery(Class<E> type, Mapping... mappingsToInsert) {
        return insertQuery(type, true, mappingsToInsert);
    }

    public InsertQuery<?> autoInsertQuery(String... mappingsToInsert) {
        return autoInsertQuery(true, mappingsToInsert);
    }

    public <E extends SQLEntity> UpdateQuery<E> updateQuery(Class<E> type, Mapping... mappingsToCompare) {
        return register(new UpdateQuery<>(this, type, simplifyMappings(mappingsToCompare)));
    }

    public UpdateQuery<?> autoUpdateQuery(String... mappingsToCompare) {
        return register(new UpdateQuery<>(this, null, mappingsToCompare));
    }

    @SuppressWarnings("unchecked")
    public <E extends SQLEntity> UpdateQuery<E> updateByIdQuery(Class<E> type, Mapping... mappingsToUpdate) {
        return register(new UpdateQuery<>(this, type, new String[]{SQLEntity.ID.getName()})).withUpdatedMappings(
                mappingsToUpdate);
    }

    @SuppressWarnings("unchecked")
    public UpdateQuery<?> autoUpdateByIdQuery(String... mappingsToUpdate) {
        return register(new UpdateQuery<>(this, null, new String[]{SQLEntity.ID.getName()}).withUpdatedMappings(
                mappingsToUpdate));
    }

    public <E extends SQLEntity> DeleteQuery<E> deleteQuery(Class<E> type, Mapping... mappingsToCompare) {
        return register(new DeleteQuery<>(this, type, simplifyMappings(mappingsToCompare)));
    }

    public DeleteQuery<?> autoDeleteQuery(String... mappingsToCompare) {
        return register(new DeleteQuery<>(this, null, mappingsToCompare));
    }

    public CustomQuery customQuery(Class<? extends SQLEntity> type, boolean fetchId, String sql) {
        return register(new CustomQuery(this, type, fetchId, sql));
    }

    @Override
    public void close() throws IOException {
        safeClose();
        op.close();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Batch Context\n");
        sb.append("----------------------------------------\n");
        for (BatchQuery<?> query : queries) {
            sb.append(query).append("\n");
        }
        sb.append("----------------------------------------\n");

        return sb.toString();
    }
}
