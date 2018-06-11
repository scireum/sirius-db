/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.alibaba.fastjson.JSONObject;
import sirius.db.mixing.EntityDescriptor;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BatchContext implements Closeable {

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

    protected BatchContext(LowLevelClient client) {
        this.maxBatchSize = DEFAULT_BATCH_SIZE;
        this.client = client;
        this.commands = new ArrayList<>();
    }

    public BatchContext tryUpdate(ElasticEntity entity) {
        update(entity, false);
        return this;
    }

    public BatchContext overwrite(ElasticEntity entity) {
        update(entity, true);
        return this;
    }

    public BatchContext tryDelete(ElasticEntity entity) {
        delete(entity, false);
        return this;
    }

    public BatchContext forceDelete(ElasticEntity entity) {
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
        boolean changed = elastic.toJSON(entity, ed, data);

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

    @Override
    public void close() throws IOException {
        commit();
    }
}
