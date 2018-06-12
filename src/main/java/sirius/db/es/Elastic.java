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

import javax.annotation.Nullable;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Provides the {@link BaseMapper mapper} used to communicate with <tt>Elasticsearch</tt>.
 */
@Register(classes = Elastic.class)
public class Elastic extends BaseMapper<ElasticEntity, ElasticQuery<? extends ElasticEntity>> {

    /**
     * Contains the logger used by everything related to Elasticsearch
     */
    public static final Log LOG = Log.get("es");

    private static final String CONTEXT_ROUTING = "routing";

    private static final String RESPONSE_VERSION = "_version";
    private static final String RESPONSE_FOUND = "found";
    private static final String RESPONSE_SOURCE = "_source";

    /**
     * Contains the name of the ID field used by Elasticsearch
     */
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

    /**
     * Provides a future which is fulfilled once the Elasticsearch client is fully initialized.
     *
     * @return a future which indicates when Elasticsearch is ready
     */
    public Future getReadyFuture() {
        return readyFuture;
    }

    /**
     * Provides access to the underlying low level client.
     *
     * @return the underlying low level client used to perform the HTTP requests against Elasticsearch.
     */
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
                waitForElasticsearchToBecomReady();
            }
        }
    }

    private void waitForElasticsearchToBecomReady() {
        int retries = 5;
        while (retries-- > 0) {
            try {
                if (client.getRestClient().performRequest("GET", "/_cat/indices").getStatusLine().getStatusCode()
                    == 200) {
                    return;
                }
            } catch (Exception e) {
                Exceptions.ignore(e);
            }
            Elastic.LOG.INFO("Sleeping two seconds to wait until Elasticsearch is ready...");
            Wait.seconds(2);
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
    protected void createEnity(ElasticEntity entity, EntityDescriptor ed) throws Exception {
        JSONObject data = new JSONObject();
        toJSON(ed, entity, data);

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

    /**
     * Determines the routing value to be used for the given entity.
     *
     * @param ed     the entity descriptor of the entity
     * @param entity the entity to fetch the routing value from
     * @return the routing value to use
     */
    @Nullable
    protected String determineRouting(EntityDescriptor ed, ElasticEntity entity) {
        Property property = routeTable.get(ed);

        if (property == null) {
            return null;
        }

        return (String) property.getValueForDatasource(entity);
    }

    @Override
    protected void updateEntity(ElasticEntity entity, boolean force, EntityDescriptor ed) throws Exception {
        JSONObject data = new JSONObject();
        boolean changed = toJSON(ed, entity, data);

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

    /**
     * Transforms the given entity to JSON.
     *
     * @param entity the entity to transform
     * @param ed     the descriptor of the entity
     * @param data   the target JSON to fill
     * @return <tt>true</tt> if at least on property has changed, <tt>false</tt> otherwise
     */
    protected boolean toJSON(EntityDescriptor ed, ElasticEntity entity, JSONObject data) {
        boolean changed = false;
        for (Property p : ed.getProperties()) {
            if (!ElasticEntity.ID.getName().equals(p.getName())) {
                data.put(p.getPropertyName(), p.getValueForDatasource(entity));
                changed |= ed.isChanged(entity, p);
            }
        }
        return changed;
    }

    /**
     * Determines the id of the entity.
     * <p>
     * This will either return the stored ID or create a new one, if the entity is still new. If the entity is
     * {@link sirius.db.es.annotations.StorePerYear stored per year}, the year will be prepended to the ID itself
     * to determine the index in {@link #determineIndex(EntityDescriptor, ElasticEntity)}.
     *
     * @param entity the entity to determine the id for
     * @return the id to use for this entity
     */
    protected String determineId(ElasticEntity entity) {
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

    /**
     * Determines the index to use for the given entity.
     * <p>
     * This will either be the {@link EntityDescriptor#getRelationName() relation name} or if the entity is
     * {@link sirius.db.es.annotations.StorePerYear stored per year}, it will be determined by
     * {@link #determineYearIndex(EntityDescriptor, Object)}.
     *
     * @param ed     the descriptor of the entity
     * @param entity the entity to determine the index for
     * @return the index name to use for the given entity.
     */
    protected String determineIndex(EntityDescriptor ed, ElasticEntity entity) {
        Property discriminator = discriminatorTable.get(ed);
        if (discriminator == null) {
            return ed.getRelationName();
        }

        int year = ((TemporalAccessor) discriminator.getValue(entity)).get(ChronoField.YEAR);
        return determineYearIndex(ed, year);
    }

    /**
     * Computes the effective index name for the given descriptor and year.
     * <p>
     * This will be {@code ed.getRelationName() + "-" + year}
     *
     * @param ed   the descriptor of the entity
     * @param year the year of the index
     * @return the index name for the given descriptor and year
     */
    protected String determineYearIndex(EntityDescriptor ed, Object year) {
        return ed.getRelationName() + "-" + year;
    }

    /**
     * Determines the type name used for a given entity type.
     *
     * @param ed the descriptor of the entity
     * @return the type name to use
     */
    protected String determineTypeName(EntityDescriptor ed) {
        return ed.getRelationName();
    }

    /**
     * Determines the version value to use for a given entity.
     *
     * @param force  <tt>true</tt> if an update should be forced
     * @param entity the entity to determine the version from
     * @return <tt>null</tt> if an update is forced or if the entity isn't {@link VersionedEntity versioned},
     * the actual entity version otherwise.
     */
    private Integer determineVersion(boolean force, ElasticEntity entity) {
        if (entity instanceof VersionedEntity && !force) {
            return ((VersionedEntity) entity).getVersion();
        }

        return null;
    }

    @Override
    protected void deleteEntity(ElasticEntity entity, boolean force, EntityDescriptor ed) throws Exception {
        getLowLevelClient().delete(determineIndex(ed, entity),
                                   determineTypeName(ed),
                                   entity.getId(),
                                   determineRouting(ed, entity),
                                   determineVersion(force, entity));
    }

    /**
     * Creates a new instance of the given entity type for the given data.
     *
     * @param ed  the descriptor of the entity type
     * @param obj the JSON data to transform
     * @return a new entity based on the given data
     */
    protected static ElasticEntity make(EntityDescriptor ed, JSONObject obj) {
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

    /**
     * Provides a "routed by" context for {@link #find(Class, Object, ContextInfo...)}.
     *
     * @param value the routing value to use
     * @return the value wrapped as context info
     */
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
            LOG.WARN("Trying to FIND an entity of type '%s' with id '%s' without providing a routing! "
                     + "This will most probably return an invalid result!\n",
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

    /**
     * Creates a {@link BulkContext batch context} used for bulk updates.
     *
     * @return a new batch context
     */
    public BulkContext batch() {
        return new BulkContext(getLowLevelClient());
    }

    /**
     * Determines if an appropriate configuration is available (e.g. a host to connect to).
     *
     * @return <tt>true</tt> if a configuration is present, <tt>false</tt> otherwise
     */
    public boolean isConfigured() {
        return !hosts.isEmpty();
    }

    /**
     * Determines if the given entity type is {@link sirius.db.es.annotations.StorePerYear stored per year}.
     *
     * @param descriptor the descriptor of the entity type
     * @return <tt>true</tt> if it is stored per year, <tt>false</tt> otherwise
     */
    public boolean isStoredPerYear(EntityDescriptor descriptor) {
        return discriminatorTable.containsKey(descriptor);
    }

    @Override
    public <E extends ElasticEntity> ElasticQuery<E> select(Class<E> type) {
        return new ElasticQuery<>(mixing.getDescriptor(type), getLowLevelClient());
    }
}
