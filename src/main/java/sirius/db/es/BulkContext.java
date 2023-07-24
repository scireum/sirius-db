/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.fasterxml.jackson.databind.node.ObjectNode;
import sirius.db.mixing.EntityDescriptor;
import sirius.kernel.commons.Json;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

/**
 * Simplifies bulk inserts, updates and deletes against Elasticsearch.
 * <p>
 * Permits to execute an arbitrary number of requests. Which will internally executed as blocks using the bulk API
 * of Elasticsearch. A certain number of commands is queued before the whole request is sent to the server. Note that
 * bulk updates may fail (e.g. due to optimistic locking). If an auto-commit fails, it will produce an exception.
 * <p>
 * Invoke {@link #commit()} manually every once in a while (ask {@link #shouldCommitManually()}). One can also
 * determine the current number of commands using {@link #countQueuedCommands()} and determine when
 * {@link #autocommit()} will be invoked (when reaching {@link #MAX_BATCH_SIZE}.
 * <p>
 * Note that {@link sirius.db.mixing.annotations.AfterSave} and {@link sirius.db.mixing.types.BaseEntityRef.OnDelete}
 * handlers are <tt>not</tt> executed!
 * <p>
 * This class is not thread-safe.
 */
@NotThreadSafe
public class BulkContext implements Closeable {

    private static final int MAX_BATCH_SIZE = 1024;
    private static final int RECOMMENDED_BATCH_SIZE = 256;

    private static final String KEY_INDEX = "_index";
    protected static final String KEY_ID = "_id";
    private static final String KEY_PRIMARY_TERM = "if_primary_term";
    private static final String KEY_SEQ_NO = "if_seq_no";
    private static final String KEY_ROUTING = "routing";

    protected static final String COMMAND_INDEX = "index";
    protected static final String COMMAND_DELETE = "delete";
    protected static final String COMMAND_CREATE = "create";
    protected static final String COMMAND_UPDATE = "update";

    private final LowLevelClient.Refresh refresh;
    private LowLevelClient client;
    private List<ObjectNode> commands;

    @Part
    private static Elastic elastic;

    /**
     * Creates a new instance using the given client.
     *
     * @param client the client used to execute the bulk requests
     * @see Elastic#batch()
     */
    protected BulkContext(LowLevelClient client, LowLevelClient.Refresh refresh) {
        this.client = client;
        this.refresh = refresh;
        this.commands = new ArrayList<>();
    }

    /**
     * Queues an {@link Elastic#tryUpdate(ElasticEntity)} in the batch context.
     *
     * @param entity the entity to create or update
     * @return the batch context itself for fluent method calls
     */
    public BulkContext tryUpdate(ElasticEntity entity) {
        update(entity, false);
        return this;
    }

    /**
     * Queues an {@link Elastic#override(ElasticEntity)} in the batch context.
     *
     * @param entity the entity to create or update
     * @return the batch context itself for fluent method calls
     */
    public BulkContext overwrite(ElasticEntity entity) {
        update(entity, true);
        return this;
    }

    /**
     * Queues an {@link Elastic#tryDelete(ElasticEntity)} in the batch context.
     *
     * @param entity the entity to delete
     * @return the batch context itself for fluent method calls
     */
    public BulkContext tryDelete(ElasticEntity entity) {
        delete(entity, false);
        return this;
    }

    /**
     * Queues an {@link Elastic#forceDelete(ElasticEntity)} in the batch context.
     *
     * @param entity the entity to delete
     * @return the batch context itself for fluent method calls
     */
    public BulkContext forceDelete(ElasticEntity entity) {
        delete(entity, true);
        return this;
    }

    private void update(ElasticEntity entity, boolean force) {
        EntityDescriptor ed = entity.getDescriptor();

        ed.beforeSave(entity);

        ObjectNode meta = builtMetadata(entity, force, ed);
        ObjectNode data = Json.createObject();
        boolean changed = elastic.toJSON(ed, entity, data);

        if (!changed) {
            return;
        }

        commands.add(Json.createObject().set(COMMAND_INDEX, meta));
        commands.add(data);
        autocommit();
    }

    private void autocommit() {
        if (commands.size() >= MAX_BATCH_SIZE) {
            commit().throwFailures();
        }
    }

    private ObjectNode builtMetadata(ElasticEntity entity, boolean force, EntityDescriptor ed) {
        ObjectNode meta = Json.createObject();

        if (!force && !entity.isNew() && ed.isVersioned()) {
            meta.put(KEY_PRIMARY_TERM, entity.getPrimaryTerm());
            meta.put(KEY_SEQ_NO, entity.getSeqNo());
        }

        entity.setId(elastic.determineId(entity));
        meta.put(KEY_INDEX, elastic.determineWriteAlias(ed));
        meta.put(KEY_ID, entity.getId());

        String routing = elastic.determineRouting(ed, entity, Elastic.RoutingAccessMode.WRITE);
        if (routing != null) {
            meta.put(KEY_ROUTING, routing);
        }
        return meta;
    }

    private void delete(ElasticEntity entity, boolean force) {
        if (entity.isNew()) {
            return;
        }

        EntityDescriptor entityDescriptor = entity.getDescriptor();
        entityDescriptor.beforeDelete(entity);

        ObjectNode meta = builtMetadata(entity, force, entityDescriptor);
        commands.add(Json.createObject().set(COMMAND_DELETE, meta));
        autocommit();
    }

    /**
     * Forces the execution of a bulk update (if statements are queued).
     *
     * @return a result which can be used to determine if errors have occurred. If an exception should be thrown for
     * any error, use {@link BulkResult#throwFailures()}.
     */
    public BulkResult commit() {
        if (commands.isEmpty()) {
            return new BulkResult(null);
        }

        try {
            ObjectNode bulkResponse = client.bulkWithRefresh(commands, refresh);
            if (Elastic.LOG.isFINE()) {
                Elastic.LOG.FINE(bulkResponse);
            }

            return new BulkResult(bulkResponse);
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(Elastic.LOG)
                            .error(e)
                            .withSystemErrorMessage(
                                    "An error occurred while executing a bulk update against Elasticsearch: %s (%s)")
                            .handle();
        } finally {
            commands.clear();
        }
    }

    /**
     * Returns the number of queued commands.
     *
     * @return the number of currently queued command
     */
    public int countQueuedCommands() {
        return commands.size();
    }

    /**
     * Determines if {@link #commit()} would be invoked manually if the caller is interested in properly handling
     * update errors.
     * <p>
     * Note that there is no need to do this. {@link #autocommit()} will invoke {@link #commit()} in sane and regular
     * intervals. However, it will always throw an exception in case of any error being encountered.
     *
     * @return <tt>true</tt> if {@link #commit()} should be called manually, <tt>false</tt> otherwise
     */
    public boolean shouldCommitManually() {
        return countQueuedCommands() >= RECOMMENDED_BATCH_SIZE;
    }

    /**
     * Returns whether any executable commands are queued.
     *
     * @return <tt>true</tt> if executable commands are queued, <tt>false</tt> otherwise.
     */
    public boolean isEmpty() {
        return commands.isEmpty();
    }

    /**
     * Closes the bulk context and executes all statements which are still queued.
     */
    @Override
    public void close() {
        commit().throwFailures();
    }
}
