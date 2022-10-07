/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.batch;

import sirius.db.jdbc.Database;
import sirius.db.jdbc.OMA;
import sirius.db.jdbc.Operator;
import sirius.db.jdbc.SQLEntity;
import sirius.db.mixing.Mapping;
import sirius.kernel.async.Operation;
import sirius.kernel.commons.Tuple;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Bundles the preparation and execution of a bunch of {@link BatchQuery batch queries}.
 * <p>
 * Note that this context can only operate on databases managed via {@link sirius.db.mixing.Mixing}. To
 * perform batch operations against external JDBC databases use {@link sirius.db.jdbc.batch.external.ExternalBatchContext}.
 */
@NotThreadSafe
public class BatchContext implements Closeable {

    @Part
    private static OMA oma;

    private List<BatchQuery<?>> queries = new ArrayList<>();
    private Map<String, Connection> connectionsPerRealm = new HashMap<>();
    private Operation op;

    /**
     * Creates a new context with the given debugging description and the expected runtime.
     *
     * @param description      a provider for a description used for debugging purposes
     * @param expectedDuration the expected duration of the whole batch operation
     */
    public BatchContext(Supplier<String> description, Duration expectedDuration) {
        this.op = new Operation(description, expectedDuration);
    }

    private <Q extends BatchQuery<?>> Q register(Q query) {
        if (queries == null) {
            reportIllegalState();
        }
        this.queries.add(query);

        return query;
    }

    private void reportIllegalState() {
        throw new IllegalStateException("This batch context has already been closed.");
    }

    protected void unregister(BatchQuery<?> query) {
        if (queries == null) {
            reportIllegalState();
        }
        this.queries.remove(query);
    }

    protected static List<Tuple<Operator, String>> simplifyMappings(Mapping[] mappingsToCompare) {
        return Arrays.stream(mappingsToCompare)
                     .map(Mapping::toString)
                     .map(mapping -> Tuple.create(Operator.EQ, mapping))
                     .toList();
    }

    protected static List<Tuple<Operator, String>> simplifyMappings(Tuple<Operator, Mapping>[] filters) {
        return Arrays.stream(filters)
                     .map(filter -> Tuple.create(filter.getFirst(), filter.getSecond().toString()))
                     .toList();
    }

    protected Connection getConnection(String realm) {
        if (connectionsPerRealm == null) {
            reportIllegalState();
        }

        return connectionsPerRealm.computeIfAbsent(realm, this::createConnection);
    }

    protected Connection createConnection(String realm) {
        try {
            Database database = oma.getDatabase(realm);
            Connection connection = database.getLongRunningConnection();
            changeAutoCommit(connection, database.isAutoCommit(), false);
            return connection;
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
        if (queries != null) {
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
        }

        if (connectionsPerRealm != null) {
            connectionsPerRealm.values().forEach(this::safeCloseConnection);
            connectionsPerRealm.clear();
        }
    }

    private void safeCloseConnection(Connection connection) {
        try {
            changeAutoCommit(connection, true, true);
            connection.close();
        } catch (SQLException e) {
            Exceptions.handle()
                      .to(OMA.LOG)
                      .error(e)
                      .withSystemErrorMessage("An exception occurred while closing a database connection: %s (%s)")
                      .handle();
        }
    }

    /**
     * Toggles the auto-commit setting for the given connection.
     * <p>
     * This is required, as otherwise batching doesn't work properly at all (if auto-commit isn't disabled).
     *
     * @param connection   the connection to update
     * @param enable       the flag which determines if auto-commit should be enabled or disabled
     * @param ignoreErrors controls whether exceptions during the change are reported or ignored. As this is invoked
     *                     when a connection is closed, we ignore any error as closing the connection might already
     *                     be part of handling a previous error (e.g. GaleraDB doesn't like changing the auto-commit
     *                     setting after a transaction has been aborted due to a deadlock).
     */
    private void changeAutoCommit(Connection connection, boolean enable, boolean ignoreErrors) {
        try {
            connection.setAutoCommit(enable);
        } catch (SQLException e) {
            if (ignoreErrors) {
                Exceptions.ignore(e);
            } else {
                Exceptions.handle()
                          .error(e)
                          .withSystemErrorMessage(
                                  "An error occurred while changing the auto-commit of %s to %s - %s (%s)",
                                  connection,
                                  enable)
                          .handle();
            }
        }
    }

    /**
     * Creates a {@link FindQuery find query}.
     *
     * @param type              the type of entities to find
     * @param mappingsToCompare the mappings to compare in order to find an entity
     * @param <E>               the generic type of the entities to find
     * @return the query used to find entities
     */
    public <E extends SQLEntity> FindQuery<E> findQuery(Class<E> type, Mapping... mappingsToCompare) {
        return register(new FindQuery<>(this, type, simplifyMappings(mappingsToCompare)));
    }

    /**
     * Creates a {@link FindQuery find query}.
     *
     * @param type    the type of entities to find
     * @param filters the mappings to compare in order to find an entity
     * @param <E>     the generic type of the entities to find
     * @return the query used to find entities
     */
    @SafeVarargs
    public final <E extends SQLEntity> FindQuery<E> findQuery(Class<E> type, Tuple<Operator, Mapping>... filters) {
        return register(new FindQuery<>(this, type, simplifyMappings(filters)));
    }

    /**
     * Creates a new {@link InsertQuery insert query}.
     *
     * @param type             the type of entities to insert
     * @param fetchId          <tt>true</tt> if generated id should be fetched, <tt>false otherwise</tt>
     * @param mappingsToInsert the fields or mappings to insert
     * @param <E>              the generic type of the entities to insert
     * @return the query used to insert entities into the database
     */
    public <E extends SQLEntity> InsertQuery<E> insertQuery(Class<E> type,
                                                            boolean fetchId,
                                                            Mapping... mappingsToInsert) {
        return register(new InsertQuery<>(this,
                                          type,
                                          fetchId,
                                          Arrays.stream(mappingsToInsert).map(Mapping::getName).toList()));
    }

    /**
     * Creates a new {@link InsertQuery insert query}.
     *
     * @param type             the type of entities to insert
     * @param mappingsToInsert the fields or mappings to insert
     * @param <E>              the generic type of the entities to insert
     * @return the query used to insert entities into the database
     */
    public <E extends SQLEntity> InsertQuery<E> insertQuery(Class<E> type, Mapping... mappingsToInsert) {
        return insertQuery(type, true, mappingsToInsert);
    }

    /**
     * Creates a new {@link UpdateQuery update query}.
     *
     * @param type              the type of entities to update
     * @param mappingsToCompare the mappings to compare in order to find the entity to update
     * @param <E>               the generic type of the entities to update
     * @return the query used to update entities in the database
     */
    public <E extends SQLEntity> UpdateQuery<E> updateQuery(Class<E> type, Mapping... mappingsToCompare) {
        return register(new UpdateQuery<>(this, type, simplifyMappings(mappingsToCompare)));
    }

    /**
     * Creates a new {@link UpdateQuery update query}.
     *
     * @param type    the type of entities to update
     * @param filters the mappings to compare in order to find the entity to update
     * @param <E>     the generic type of the entities to update
     * @return the query used to update entities in the database
     */
    @SafeVarargs
    public final <E extends SQLEntity> UpdateQuery<E> updateQuery(Class<E> type, Tuple<Operator, Mapping>... filters) {
        return register(new UpdateQuery<>(this, type, simplifyMappings(filters)));
    }

    /**
     * Creates a new {@link UpdateQuery update query} which uses {@link SQLEntity#ID} as mapping to compare.
     *
     * @param type             the type of entities to insert
     * @param mappingsToUpdate the mappings to update
     * @param <E>              the generic type of the entities to update
     * @return the query used to update entities in the database
     */
    public <E extends SQLEntity> UpdateQuery<E> updateByIdQuery(Class<E> type, Mapping... mappingsToUpdate) {
        UpdateQuery<E> updateQuery = new UpdateQuery<>(this,
                                                       type,
                                                       Collections.singletonList(Tuple.create(Operator.EQ,
                                                                                              SQLEntity.ID.getName())));
        updateQuery.withUpdatedMappings(mappingsToUpdate);
        return register(updateQuery);
    }

    /**
     * Creates a new {@link DeleteQuery delete query}.
     *
     * @param type              the type of entities to delete
     * @param mappingsToCompare the mappings to compare in order to find the entity to delete
     * @param <E>               the generic type of the entities to delete
     * @return the query used to delete entities in the database
     */
    public <E extends SQLEntity> DeleteQuery<E> deleteQuery(Class<E> type, Mapping... mappingsToCompare) {
        return register(new DeleteQuery<>(this, type, simplifyMappings(mappingsToCompare)));
    }

    /**
     * Creates a new {@link DeleteQuery delete query}.
     *
     * @param type    the type of entities to delete
     * @param filters the mappings to compare in order to find the entity to delete
     * @param <E>     the generic type of the entities to delete
     * @return the query used to delete entities in the database
     */
    @SafeVarargs
    public final <E extends SQLEntity> DeleteQuery<E> deleteQuery(Class<E> type, Tuple<Operator, Mapping>... filters) {
        return register(new DeleteQuery<>(this, type, simplifyMappings(filters)));
    }

    /**
     * Prepares the given SQL statement as {@link CustomQuery custom query}.
     *
     * @param type    the type of entities to process
     * @param fetchId determines if generated IDs should be fetched
     * @param sql     the statement to prepare
     * @return the prepared statement wrapped as custom query
     */
    public CustomQuery customQuery(Class<? extends SQLEntity> type, boolean fetchId, String sql) {
        return register(new CustomQuery(this, type, fetchId, sql));
    }

    /**
     * Invokes {@link BatchQuery#tryCommit(boolean)} for all open queries.
     * <p>
     * Note that in most cases the automatic commit control of the batch context takes good control over when to
     * commit which query. This is mostly useful for edge cases or tests.
     */
    public void tryCommit() {
        if (queries == null) {
            return;
        }

        for (BatchQuery<?> query : queries) {
            try {
                query.tryCommit(false);
            } catch (Exception e) {
                throw Exceptions.handle()
                                .to(OMA.LOG)
                                .error(e)
                                .withSystemErrorMessage("An error occurred when flushing the BatchContext: %s (%s)")
                                .handle();
            }
        }
    }

    @Override
    public void close() throws IOException {
        safeClose();

        // Mark this context as closed so that no further queries or connections can be opened after
        // this has been completed...
        queries = null;
        connectionsPerRealm = null;

        op.close();
    }

    /**
     * Determines if the batch context is empty.
     *
     * @return <tt>true</tt> if there are no queries registered at all, <tt>false</tt> otherwise
     */
    public boolean isEmpty() {
        return this.queries.stream().noneMatch(BatchQuery::isQueryAvailable);
    }

    @Override
    public String toString() {
        if (this.queries == null) {
            return "Closed";
        }
        if (isEmpty()) {
            return "Empty batch context";
        }

        StringBuilder sb = new StringBuilder("Batch Context\n");
        sb.append("----------------------------------------\n");
        for (BatchQuery<?> query : queries) {
            if (query.isQueryAvailable()) {
                sb.append(query).append("\n");
            }
        }
        sb.append("----------------------------------------\n");

        return sb.toString();
    }
}
