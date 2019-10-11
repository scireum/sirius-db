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
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import sirius.db.KeyGenerator;
import sirius.db.es.constraints.ElasticConstraint;
import sirius.db.es.constraints.ElasticFilterFactory;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.ContextInfo;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.Property;
import sirius.db.mixing.query.constraints.FilterFactory;
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
import sirius.kernel.health.Counter;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Log;
import sirius.kernel.settings.PortMapper;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Provides the {@link BaseMapper mapper} used to communicate with <tt>Elasticsearch</tt>.
 */
@Register(classes = Elastic.class)
public class Elastic extends BaseMapper<ElasticEntity, ElasticConstraint, ElasticQuery<? extends ElasticEntity>> {

    /**
     * Contains the logger used by everything related to Elasticsearch
     */
    public static final Log LOG = Log.get("es");

    /**
     * Constains the factory used to generate filters for a {@link ElasticQuery}.
     */
    public static final ElasticFilterFactory FILTERS = new ElasticFilterFactory();

    /**
     * The suffix of the alias which resolves the currently active index for a {@link EntityDescriptor}.
     */
    public static final String ACTIVE_ALIAS = "-active";

    private static final String CONTEXT_ROUTING = "routing";

    private static final String RESPONSE_VERSION = "_version";
    private static final String RESPONSE_FOUND = "found";
    private static final String RESPONSE_SOURCE = "_source";
    private static final String MATCHED_QUERIES = "matched_queries";

    /**
     * Contains the name of the ID field used by Elasticsearch
     */
    public static final String ID_FIELD = "_id";

    private static final int DEFAULT_HTTP_PORT = 9200;

    private static final String SERVICE_ELASTICSEARCH = "elasticsearch";
    private static final String SCHEME_HTTP = "http";

    @Part
    private IndexNaming indexNaming;

    @Part
    private KeyGenerator keyGen;

    @Part
    private IndexMappings indexMappings;

    @ConfigValue("elasticsearch.hosts")
    private String hosts;

    @ConfigValue("elasticsearch.logQueryThreshold")
    private static Duration logQueryThreshold;
    private static long logQueryThresholdMillis = -1;

    private LowLevelClient client;

    protected Future readyFuture = new Future();
    protected Average callDuration = new Average();
    protected Counter numSlowQueries = new Counter();
    protected Map<EntityDescriptor, Property> routeTable = new HashMap<>();
    protected boolean dockerDetected = false;

    protected void updateRouteTable(EntityDescriptor ed, Property p) {
        routeTable.put(ed, p);
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

            // Fixes an Elastic bug that results in TimeoutExceptions
            // Remove this, once ES is updated to at least 6.3.1
            RestClientBuilder.RequestConfigCallback configCallback =
                    requestConfigBuilder -> requestConfigBuilder.setConnectionRequestTimeout(0);

            HttpHost[] httpHosts = Arrays.stream(this.hosts.split(","))
                                     .map(String::trim)
                                     .map(host -> Strings.splitAtLast(host, ":"))
                                     .map(this::parsePort)
                                     .map(this::mapPort)
                                     .map(this::makeHttpHost)
                                     .toArray(size -> new HttpHost[size]);
            client = new LowLevelClient(RestClient.builder(httpHosts).setRequestConfigCallback(configCallback).build());

            // If we're using a docker container (most probably for testing), we give ES some time
            // to fully boot up. Otherwise strange connection issues might arise.
            if (dockerDetected) {
                waitForElasticsearchToBecomReady();
            }
        }
    }

    private void waitForElasticsearchToBecomReady() {
        int retries = 15;
        while (retries-- > 0) {
            try {
                if (client.getRestClient()
                          .performRequest(new Request("GET", "/_cat/indices"))
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

        Elastic.LOG.WARN("Elasticsearch was not ready after waiting 30s...");
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

    private Tuple<String, Integer> mapPort(Tuple<String, Integer> hostAndPort) {
        Tuple<String, Integer> effectiveHostAndPort =
                PortMapper.mapPort(SERVICE_ELASTICSEARCH, hostAndPort.getFirst(), hostAndPort.getSecond());
        if (!Objects.equals(effectiveHostAndPort.getSecond(), hostAndPort.getSecond())) {
            dockerDetected = true;
        }

        return effectiveHostAndPort;
    }

    private HttpHost makeHttpHost(Tuple<String, Integer> hostnameAndPort) {
        return new HttpHost(hostnameAndPort.getFirst(), hostnameAndPort.getSecond(), SCHEME_HTTP);
    }

    @Override
    protected void createEntity(ElasticEntity entity, EntityDescriptor ed) throws Exception {
        JSONObject data = new JSONObject();
        toJSON(ed, entity, data);

        String id = determineId(entity);
        JSONObject response = getLowLevelClient().index(determineAlias(ed),
                                                        determineTypeName(ed),
                                                        id,
                                                        determineRouting(ed, entity),
                                                        null,
                                                        data);
        entity.setId(id);
        if (ed.isVersioned()) {
            entity.setVersion(response.getInteger(RESPONSE_VERSION));
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

        return (String) property.getValueForDatasource(Elastic.class, entity);
    }

    @Override
    protected void updateEntity(ElasticEntity entity, boolean force, EntityDescriptor ed) throws Exception {
        JSONObject data = new JSONObject();
        boolean changed = toJSON(ed, entity, data);

        if (!changed) {
            return;
        }

        JSONObject response = getLowLevelClient().index(determineAlias(ed),
                                                        determineTypeName(ed),
                                                        determineId(entity),
                                                        determineRouting(ed, entity),
                                                        determineVersion(force, ed, entity),
                                                        data);

        if (ed.isVersioned()) {
            entity.setVersion(response.getInteger(RESPONSE_VERSION));
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
                data.put(p.getPropertyName(), p.getValueForDatasource(Elastic.class, entity));
                changed |= ed.isChanged(entity, p);
            }
        }
        return changed;
    }

    /**
     * Determines the id of the entity.
     * <p>
     * This will either return the stored ID or create a new one, if the entity is still new.
     *
     * @param entity the entity to determine the id for
     * @return the id to use for this entity
     */
    protected String determineId(ElasticEntity entity) {
        if (entity.isNew()) {
            return keyGen.generateId();
        }

        return entity.getId();
    }

    /**
     * Determines the index to use for the given entity.
     * <p>
     * It will be determined by {@link IndexNaming} if it is implemented, or it will equal the
     * {@link EntityDescriptor#getRelationName() relation name}.
     *
     * @param ed the descriptor of the entity
     * @return the index name to use for the given entity.
     */
    protected String determineIndex(EntityDescriptor ed) {
        if (indexNaming == null) {
            return ed.getRelationName();
        }

        return indexNaming.determineIndexName(ed);
    }

    /**
     * Determines the alias for the currently active index for the given {@link EntityDescriptor}.
     *
     * @param ed the descriptor
     * @return the alias of the currently active index
     */
    protected String determineAlias(EntityDescriptor ed) {
        return ed.getRelationName() + ACTIVE_ALIAS;
    }

    /**
     * Determines the type name used for a given entity type.
     * <p>
     * It will be determined by {@link IndexNaming} if it is implemented, or it will equal the lowercase
     * {@link Class#getSimpleName() name of the entity}.
     *
     * @param ed the descriptor of the entity
     * @return the type name to use
     */
    protected String determineTypeName(EntityDescriptor ed) {
        if (indexNaming == null) {
            return ed.getType().getSimpleName().toLowerCase();
        }

        return indexNaming.determineMappingName(ed);
    }

    /**
     * Determines the version value to use for a given entity.
     *
     * @param force  <tt>true</tt> if an update should be forced
     * @param entity the entity to determine the version from
     * @return <tt>null</tt> if an update is forced or if the entity isn't
     * {@link sirius.db.mixing.annotations.Versioned}, the actual entity version otherwise.
     */
    private Integer determineVersion(boolean force, EntityDescriptor ed, ElasticEntity entity) {
        if (ed.isVersioned() && !force) {
            return entity.getVersion();
        }

        return null;
    }

    @Override
    protected void deleteEntity(ElasticEntity entity, boolean force, EntityDescriptor ed) throws Exception {
        getLowLevelClient().delete(determineAlias(ed),
                                   determineTypeName(ed),
                                   entity.getId(),
                                   determineRouting(ed, entity),
                                   determineVersion(force, ed, entity));
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
            JSONArray matchedQueries = obj.getJSONArray(MATCHED_QUERIES);

            ElasticEntity result = (ElasticEntity) ed.make(Elastic.class, null, key -> Value.of(source.get(key)));
            result.setId(obj.getString(ID_FIELD));

            if (matchedQueries != null) {
                result.setMatchedQueries(new HashSet<>(matchedQueries.toJavaList(String.class)));
            }

            if (ed.isVersioned()) {
                result.setVersion(obj.getInteger(RESPONSE_VERSION));
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
                     + "This will most probably return an invalid result!\n%s",
                     ed.getType().getName(),
                     id,
                     ExecutionPoint.snapshot());
        }

        JSONObject obj =
                getLowLevelClient().get(determineAlias(ed), determineTypeName(ed), id.toString(), routing, true);

        if (obj == null || !Boolean.TRUE.equals(obj.getBoolean(RESPONSE_FOUND))) {
            return Optional.empty();
        }

        E result = (E) make(ed, obj);
        return Optional.of(result);
    }

    /**
     * Determines if the entity of the given descriptor requires a routing value.
     *
     * @param ed the descriptor of the entity to check
     * @return <tt>true</tt> if a routing is required, <tt>false</tt> otherwise
     */
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
        return Strings.isFilled(hosts);
    }

    /**
     * Returns the query log threshold in millis.
     * <p>
     * If the execution duration of a query is longer than this threshold, it is logged into
     * {@link sirius.db.DB#SLOW_DB_LOG} for further analysis.
     *
     * @return the log thresold for queries in milliseconds
     */
    protected static long getLogQueryThresholdMillis() {
        if (logQueryThresholdMillis < 0) {
            logQueryThresholdMillis = logQueryThreshold.toMillis();
        }

        return logQueryThresholdMillis;
    }

    @Override
    public <E extends ElasticEntity> ElasticQuery<E> select(Class<E> type) {
        return new ElasticQuery<>(mixing.getDescriptor(type), getLowLevelClient());
    }

    /**
     * Allows to explicitly refresh the index for the given {@link ElasticEntity}, making all operations performed
     * since the last refresh available for search.
     *
     * @param type the entity type which should be refreshed
     * @param <E>  the concrete type which should be refreshed
     */
    public <E extends ElasticEntity> void refresh(Class<E> type) {
        getLowLevelClient().refresh(determineIndex(mixing.getDescriptor(type)));
    }

    @Override
    public FilterFactory<ElasticConstraint> filters() {
        return FILTERS;
    }

    @Override
    public Value fetchField(Class<? extends ElasticEntity> type, Object id, Mapping field) throws Exception {
        throw new UnsupportedOperationException();
    }
}
