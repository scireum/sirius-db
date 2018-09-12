/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.EntityDescriptor;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Simplifies bulk inserts, updates and deletes against Elasticsearch.
 * <p>
 * Permits to execute an arbitrary number of requests. Which will internally executed as blocks using the bulk API
 * of Elasticsearch.
 * <p>
 * Note that this instance isn't threadsafe.
 * <p>
 * Note that {@link sirius.db.mixing.annotations.AfterSave} and {@link sirius.db.mixing.types.BaseEntityRef.OnDelete}
 * handlers are <tt>not</tt> executed!
 */
@NotThreadSafe
public class BulkContext implements Closeable {

    private static final int DEFAULT_BATCH_SIZE = 256;

    private static final String KEY_INDEX = "_index";
    private static final String KEY_TYPE = "_type";
    private static final String KEY_ID = "_id";
    private static final String KEY_VERSION = "_version";
    private static final String KEY_ROUTING = "_routing";
    private static final String KEY_ERROR = "error";
    private static final String KEY_ITEMS = "items";

    private static final String COMMAND_INDEX = "index";
    private static final String COMMAND_DELETE = "delete";
    private static final String COMMAND_CREATE = "create";
    private static final String COMMAND_UPDATE = "update";

    private static final String RESPONSE_INDEX = "index";
    private static final String RESPONSE_TYPE = "type";
    private static final String RESPONSE_REASON = "reason";
    private static final String RESPONSE_CAUSED_BY = "caused_by";

    private final int maxBatchSize;
    private LowLevelClient client;
    private List<JSONObject> commands;
    private JSONObject response;
    private Set<String> failedIds;
    private String failureMessage;

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

        ed.beforeSave(entity);

        JSONObject meta = new JSONObject();

        if (!force && !entity.isNew() && ed.isVersioned()) {
            meta.put(KEY_VERSION, entity.getVersion());
        }

        entity.setId(elastic.determineId(entity));
        meta.put(KEY_INDEX, elastic.determineIndex(ed, entity));
        meta.put(KEY_TYPE, elastic.determineTypeName(ed));
        meta.put(KEY_ID, entity.getId());

        String routing = elastic.determineRouting(ed, entity);
        if (routing != null) {
            meta.put(KEY_ROUTING, routing);
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

        ed.beforeDelete(entity);

        JSONObject meta = new JSONObject();

        if (!force && ed.isVersioned()) {
            meta.put(KEY_VERSION, entity.getVersion());
        }

        entity.setId(elastic.determineId(entity));
        meta.put(KEY_INDEX, elastic.determineIndex(ed, entity));
        meta.put(KEY_TYPE, elastic.determineTypeName(ed));
        meta.put(KEY_ID, entity.getId());

        String routing = elastic.determineRouting(ed, entity);
        if (routing != null) {
            meta.put(KEY_ROUTING, routing);
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
            failedIds = null;

            JSONObject bulkResponse = client.bulk(commands);
            if (Elastic.LOG.isFINE()) {
                Elastic.LOG.FINE(bulkResponse);
            }

            this.response = bulkResponse;
            boolean hasErrors = bulkResponse.getBooleanValue("errors");

            if (Strings.isFilled(getFailureMessage())) {
                Exceptions.handle().withSystemErrorMessage(getFailureMessage()).handle();
            }

            return hasErrors;
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
     * Returns all _id-fields of sub-requests which failed within this bulk request.
     *
     * @return a {@link Set} of _ids for which the bulk request failed.
     */
    public Set<String> getFailedIds() {
        if (response == null || !response.getBooleanValue("errors")) {
            return Collections.emptySet();
        }

        if (failedIds != null) {
            return Collections.unmodifiableSet(failedIds);
        }

        StringBuilder failureMessageBuilder = new StringBuilder();
        this.failedIds = new HashSet<>();
        JSONArray items = response.getJSONArray(KEY_ITEMS);

        for (int i = 0; i < items.size(); i++) {
            JSONObject current = getObject(items.getJSONObject(i));
            JSONObject error = current.getJSONObject(KEY_ERROR);
            if (error != null) {
                failedIds.add(current.getString(KEY_ID));
                failureMessageBuilder.append("index: ")
                                     .append(error.getString(RESPONSE_INDEX))
                                     .append(" type: ")
                                     .append(error.getString(RESPONSE_TYPE))
                                     .append(" reason: ")
                                     .append(error.getString(RESPONSE_REASON));
                if (error.getJSONObject(RESPONSE_CAUSED_BY) != null) {
                    failureMessageBuilder.append(" cause: ")
                                         .append(error.getJSONObject(RESPONSE_CAUSED_BY).getString(RESPONSE_REASON));
                }
                failureMessageBuilder.append("\n");
            }
        }

        failureMessage = failureMessageBuilder.toString();

        return Collections.unmodifiableSet(failedIds);
    }

    /**
     * Returns the failure message for this bulk request.
     *
     * @return the failure message if errors occured. Otherwise an empty string.
     */
    public String getFailureMessage() {
        if (failedIds == null) {
            getFailedIds();
        }

        if (Strings.isEmpty(failureMessage)) {
            return "";
        }

        return failureMessage;
    }

    private JSONObject getObject(JSONObject currentObject) {
        JSONObject object = currentObject.getJSONObject(COMMAND_INDEX);
        if (object != null) {
            return object;
        }

        object = currentObject.getJSONObject(COMMAND_DELETE);
        if (object != null) {
            return object;
        }

        object = currentObject.getJSONObject(COMMAND_CREATE);
        if (object != null) {
            return object;
        }

        object = currentObject.getJSONObject(COMMAND_UPDATE);
        if (object != null) {
            return object;
        }

        throw Exceptions.handle().withSystemErrorMessage("Unknown object type within bulk-response!").handle();
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
        commit();
    }
}
