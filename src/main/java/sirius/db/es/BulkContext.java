/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.alibaba.fastjson.JSONObject;
import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.EntityDescriptor;
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
 * of Elasticsearch.
 * <p>
 * Note that this instance isn't threadsafe.
 */
@NotThreadSafe
public class BulkContext implements Closeable {

    private static final int DEFAULT_BATCH_SIZE = 256;

    private static final String KEY_INDEX = "_index";
    private static final String KEY_TYPE = "_type";
    private static final String KEY_ID = "_id";
    private static final String KEY_VERSION = "_version";
    private static final String COMMAND_INDEX = "index";
    private static final String COMMAND_DELETE = "delete";
    private static final String KEY_ROUTING = "_routing";

    private final int maxBatchSize;
    private LowLevelClient client;
    private List<JSONObject> commands;

    @Part
    private static Elastic elastic;

    /**
     * Creates a new instance using the given client.
     *
     * @param client the client used to execute the bulk requests
     * @see Elastic#batch()
     */
    protected BulkContext(LowLevelClient client) {
        this.maxBatchSize = DEFAULT_BATCH_SIZE;
        this.client = client;
        this.commands = new ArrayList<>();
    }

    /**
     * Queues an {@link Elastic#tryUpdate(BaseEntity)} in the batch context.
     *
     * @param entity the entity to create or update
     * @return the batch context itself for fluent method calls
     */
    public BulkContext tryUpdate(ElasticEntity entity) {
        update(entity, false);
        return this;
    }

    /**
     * Queues an {@link Elastic#override(BaseEntity)} in the batch context.
     *
     * @param entity the entity to create or update
     * @return the batch context itself for fluent method calls
     */
    public BulkContext overwrite(ElasticEntity entity) {
        update(entity, true);
        return this;
    }

    /**
     * Queues an {@link Elastic#tryDelete(BaseEntity)} in the batch context.
     *
     * @param entity the entity to delete
     * @return the batch context itself for fluent method calls
     */
    public BulkContext tryDelete(ElasticEntity entity) {
        delete(entity, false);
        return this;
    }

    /**
     * Queues an {@link Elastic#forceDelete(BaseEntity)} in the batch context.
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
        JSONObject meta = new JSONObject();
        meta.put(KEY_INDEX, elastic.determineIndex(ed, entity));
        meta.put(KEY_TYPE, elastic.determineTypeName(ed));
        meta.put(KEY_ID, elastic.determineId(entity));

        String routing = elastic.determineRouting(ed, entity);
        if (routing != null) {
            meta.put(KEY_ROUTING, routing);
        }

        if (!force && !entity.isNew() && entity instanceof VersionedEntity) {
            meta.put(KEY_VERSION, ((VersionedEntity) entity).getVersion());
        }

        JSONObject data = new JSONObject();
        boolean changed = elastic.toJSON(ed, entity, data);

        if (!changed) {
            return;
        }

        commands.add(new JSONObject().fluentPut(COMMAND_INDEX, meta));
        commands.add(data);
    }

    private void delete(ElasticEntity entity, boolean force) {
        if (entity.isNew()) {
            return;
        }

        EntityDescriptor ed = entity.getDescriptor();
        JSONObject meta = new JSONObject();
        meta.put(KEY_INDEX, elastic.determineIndex(ed, entity));
        meta.put(KEY_TYPE, elastic.determineTypeName(ed));
        meta.put(KEY_ID, elastic.determineId(entity));

        String routing = elastic.determineRouting(ed, entity);
        if (routing != null) {
            meta.put(KEY_ROUTING, routing);
        }

        if (!force && entity instanceof VersionedEntity) {
            meta.put(KEY_VERSION, ((VersionedEntity) entity).getVersion());
        }

        commands.add(new JSONObject().fluentPut(COMMAND_DELETE, meta));
    }

    private void autocommit() {
        if (commands.size() >= maxBatchSize) {
            commit();
        }
    }

    /**
     * Forces the execution of a bulk update (if statements are queued).
     *
     * @return <tt>true</tt> if errors occurred, <tt>false</tt> otherwise
     */
    public boolean commit() {
        if (commands.isEmpty()) {
            return false;
        }

        try {
            JSONObject response = client.bulk(commands);
            if (Elastic.LOG.isFINE()) {
                Elastic.LOG.FINE(response);
            }
            return response.getBooleanValue("errors");
        } catch (Exception e) {
            Exceptions.handle()
                      .to(Elastic.LOG)
                      .error(e)
                      .withSystemErrorMessage(
                              "An error occurred while executing a bulk update against Elasticsearch: %s (%s)")
                      .handle();
            return true;
        } finally {
            commands.clear();
        }
    }

    /**
     * Closes the bulk context and executes all statements which are still queued.
     */
    @Override
    public void close() {
        commit();
    }
}
