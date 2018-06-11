/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import sirius.db.KeyGenerator;
import sirius.db.es.query.ElasticQuery;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.ContextInfo;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Property;
import sirius.kernel.async.ExecutionPoint;
import sirius.kernel.async.Future;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Value;
import sirius.kernel.commons.Wait;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Average;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Log;
import sirius.kernel.settings.PortMapper;

import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

@Register(classes = Elastic.class)
public class Elastic extends BaseMapper<ElasticEntity, ElasticQuery<? extends ElasticEntity>> {

    public static final Log LOG = Log.get("es");

    private static final String CONTEXT_ROUTING = "routing";

    private static final String RESPONSE_VERSION = "_version";
    private static final String RESPONSE_FOUND = "found";
    private static final String RESPONSE_SOURCE = "_source";
    public static final String ID_FIELD = "_id";

    private static final int DEFAULT_HTTP_PORT = 9200;

    private static final String SERVICE_ELASTICSEARCH = "elasticsearch";
    private static final String SCHEME_HTTP = "http";

    @Part
    private KeyGenerator keyGen;

    @Part
    private IndexMappings indexMappings;

    @ConfigValue("elasticsearch.hosts")
    private List<String> hosts;

    private LowLevelClient client;

    protected Future readyFuture = new Future();
    protected Average callDuration = new Average();
    protected Map<EntityDescriptor, Property> routeTable = new HashMap<>();
    protected Map<EntityDescriptor, Property> discriminatorTable = new HashMap<>();
    protected boolean dockerDetected = false;

    protected void updateRouteTable(EntityDescriptor ed, Property p) {
        routeTable.put(ed, p);
    }

    protected void updateDiscriminatorTable(EntityDescriptor ed, Property p) {
        discriminatorTable.put(ed, p);
    }

    public Future getReadyFuture() {
        return readyFuture;
    }

    public LowLevelClient getLowLevelClient() {
        if (client == null) {
            initializeClient();
        }

        return client;
    }

    private synchronized void initializeClient() {
        if (client == null) {
            Elastic.LOG.INFO("Initializing Elasticsearch client against: %s", hosts);
            client = new LowLevelClient(RestClient.builder(hosts.stream()
                                                                .map(host -> Strings.splitAtLast(host, ":"))
                                                                .map(this::parsePort)
                                                                .map(this::mapPort)
                                                                .map(this::makeHttpHost)
                                                                .toArray(size -> new HttpHost[size])).build());

            // If we're using a docker container (most probably for testing), we give ES some time
            // to fully boot up. Otherwise strange connection issues might arise.
            if (dockerDetected) {
                int retries = 5;
                while (retries-- > 0) {
                    try {
                        if (client.getRestClient()
                                  .performRequest("GET", "/_cat/indices")
                                  .getStatusLine()
                                  .getStatusCode() == 200) {
                            return;
                        }
                    } catch (Exception e) {
                        Exceptions.ignore(e);
                    }
                    Elastic.LOG.INFO("Sleeping two seconds to wait until Elasticsearch is ready...");
                    Wait.seconds(2);
                }
            }
        }
    }

    private Tuple<String, Integer> parsePort(Tuple<String, String> hostnameAndPort) {
        if (Strings.isFilled(hostnameAndPort.getSecond())) {
            try {
                return Tuple.create(hostnameAndPort.getFirst(), Integer.parseInt(hostnameAndPort.getSecond()));
            } catch (NumberFormatException e) {
                Exceptions.handle()
                          .to(LOG)
                          .withSystemErrorMessage("Invalid port in 'elasticsearch.hosts': %s %s",
                                                  hostnameAndPort.getFirst(),
                                                  hostnameAndPort.getSecond())
                          .handle();
            }
        }

        return Tuple.create(hostnameAndPort.getFirst(), DEFAULT_HTTP_PORT);
    }

    private Tuple<String, Integer> mapPort(Tuple<String, Integer> hostnameAndPort) {
        int effectivePort = PortMapper.mapPort(SERVICE_ELASTICSEARCH, hostnameAndPort.getSecond());
        if (effectivePort != hostnameAndPort.getSecond()) {
            dockerDetected = true;
        }

        return Tuple.create(hostnameAndPort.getFirst(), effectivePort);
    }

    private HttpHost makeHttpHost(Tuple<String, Integer> hostnameAndPort) {
        return new HttpHost(hostnameAndPort.getFirst(), hostnameAndPort.getSecond(), SCHEME_HTTP);
    }

    @Override
    protected <E extends ElasticEntity> void createEnity(E entity, EntityDescriptor ed) throws Exception {
        JSONObject data = new JSONObject();
        toJSON(entity, ed, data);

        String id = determineId(entity);
        JSONObject response = getLowLevelClient().index(determineIndex(ed, entity),
                                                        determineTypeName(ed),
                                                        id,
                                                        determineRouting(ed, entity),
                                                        null,
                                                        data);
        entity.setId(id);
        if (entity instanceof VersionedEntity) {
            ((VersionedEntity) entity).setVersion(response.getInteger(RESPONSE_VERSION));
        }
    }

    public String determineRouting(EntityDescriptor ed, ElasticEntity entity) {
        Property property = routeTable.get(ed);

        if (property == null) {
            return null;
        }

        return (String) property.getValueForDatasource(entity);
    }

    @Override
    protected <E extends ElasticEntity> void updateEntity(E entity, boolean force, EntityDescriptor ed)
            throws Exception {
        JSONObject data = new JSONObject();
        boolean changed = toJSON(entity, ed, data);

        if (!changed) {
            return;
        }

        JSONObject response = getLowLevelClient().index(determineIndex(ed, entity),
                                                        determineTypeName(ed),
                                                        determineId(entity),
                                                        determineRouting(ed, entity),
                                                        determineVersion(force, entity),
                                                        data);

        if (entity instanceof VersionedEntity) {
            ((VersionedEntity) entity).setVersion(response.getInteger(RESPONSE_VERSION));
        }
    }

    public boolean toJSON(ElasticEntity entity, EntityDescriptor ed, JSONObject data) {
        boolean changed = false;
        for (Property p : ed.getProperties()) {
            if (!ElasticEntity.ID.getName().equals(p.getName())) {
                data.put(p.getPropertyName(), p.getValueForDatasource(entity));
                changed |= ed.isChanged(entity, p);
            }
        }
        return changed;
    }

    public String determineId(ElasticEntity entity) {
        if (entity.isNew()) {
            EntityDescriptor ed = entity.getDescriptor();
            Property discriminator = discriminatorTable.get(ed);
            if (discriminator == null) {
                return keyGen.generateId();
            }

            int year = ((TemporalAccessor) discriminator.getValue(entity)).get(ChronoField.YEAR);
            indexMappings.ensureYearlyIndexExists(ed, year);
            return year + keyGen.generateId();
        }

        return entity.getId();
    }

    public String determineIndex(EntityDescriptor ed, ElasticEntity entity) {
        Property discriminator = discriminatorTable.get(ed);
        if (discriminator == null) {
            return ed.getRelationName();
        }

        int year = ((TemporalAccessor) discriminator.getValue(entity)).get(ChronoField.YEAR);
        return determineYearIndex(ed, year);
    }

    public String determineYearIndex(EntityDescriptor ed, Object year) {
        return ed.getRelationName() + "-" + year;
    }

    public String determineTypeName(EntityDescriptor ed) {
        return ed.getRelationName();
    }

    private Integer determineVersion(boolean force, ElasticEntity entity) {
        if (entity instanceof VersionedEntity && !force) {
            return ((VersionedEntity) entity).getVersion();
        }

        return null;
    }

    @Override
    protected <E extends ElasticEntity> void deleteEntity(E entity, boolean force, EntityDescriptor ed)
            throws Exception {
        getLowLevelClient().delete(determineIndex(ed, entity),
                                   determineTypeName(ed),
                                   entity.getId(),
                                   determineRouting(ed, entity),
                                   determineVersion(force, entity));
    }

    public static ElasticEntity make(EntityDescriptor ed, JSONObject obj) {
        try {
            JSONObject source = obj.getJSONObject(RESPONSE_SOURCE);
            ElasticEntity result = (ElasticEntity) ed.make(null, key -> Value.of(source.get(key)));
            result.setId(obj.getString(ID_FIELD));

            if (result instanceof VersionedEntity) {
                ((VersionedEntity) result).setVersion(obj.getInteger(RESPONSE_VERSION));
            }

            return result;
        } catch (Exception e) {
            throw Exceptions.handle(Elastic.LOG, e);
        }
    }

    public static ContextInfo routedBy(String value) {
        return new ContextInfo(CONTEXT_ROUTING, Value.of(value));
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <E extends ElasticEntity> Optional<E> findEntity(Object id,
                                                               EntityDescriptor ed,
                                                               Function<String, Value> context) throws Exception {
        String routing = context.apply(CONTEXT_ROUTING).getString();

        if (routing == null && isRouted(ed)) {
            LOG.WARN(
                    "Trying to FIND and entity of type '%s' with id '%s' without providing a routing! This will most probably return an invalid result!\n",
                    ed.getType().getName(),
                    id,
                    ExecutionPoint.snapshot());
        }

        String index =
                isStoredPerYear(ed) ? determineYearIndex(ed, id.toString().substring(0, 4)) : determineIndex(ed, null);
        JSONObject obj = getLowLevelClient().get(index, determineTypeName(ed), id.toString(), routing, true);

        if (obj == null || !Boolean.TRUE.equals(obj.getBoolean(RESPONSE_FOUND))) {
            return Optional.empty();
        }

        E result = (E) make(ed, obj);
        return Optional.of(result);
    }

    public boolean isRouted(EntityDescriptor ed) {
        return routeTable.containsKey(ed);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <E extends ElasticEntity> Optional<E> findEntity(E entity) {
        return find((Class<E>) entity.getClass(),
                    entity.getId(),
                    routedBy(determineRouting(entity.getDescriptor(), entity)));
    }

    public BatchContext batch() {
        return new BatchContext(getLowLevelClient());
    }

    public boolean isConfigured() {
        return !hosts.isEmpty();
    }

    public boolean isStoredPerYear(EntityDescriptor descriptor) {
        return discriminatorTable.containsKey(descriptor);
    }

    @Override
    public <E extends ElasticEntity> ElasticQuery<E> select(Class<E> type) {
        return new ElasticQuery<>(mixing.getDescriptor(type), getLowLevelClient());
    }
}
