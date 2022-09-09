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
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import sirius.db.KeyGenerator;
import sirius.db.es.constraints.ElasticConstraint;
import sirius.db.es.constraints.ElasticFilterFactory;
import sirius.db.es.suggest.SuggestionQuery;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.ContextInfo;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.Property;
import sirius.kernel.async.ExecutionPoint;
import sirius.kernel.async.Future;
import sirius.kernel.commons.Explain;
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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
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
     * Contains the factory used to generate filters for a {@link ElasticQuery}.
     */
    public static final ElasticFilterFactory FILTERS = new ElasticFilterFactory();

    /**
     * The suffix of the alias which resolves the currently active index for a {@link EntityDescriptor}.
     */
    public static final String ACTIVE_ALIAS = "-active";

    private static final String CONTEXT_ROUTING = "routing";

    private static final String RESPONSE_PRIMARY_TERM = "_primary_term";
    private static final String RESPONSE_SEQ_NO = "_seq_no";
    private static final String RESPONSE_FOUND = "found";
    private static final String RESPONSE_SOURCE = "_source";

    /**
     * Contains the name of the ID field used by Elasticsearch
     */
    public static final String ID_FIELD = "_id";

    /**
     * Contains the ID field as mapping.
     * <p>
     * This can be used to sort by to yield unique sort fields to be used with
     * {@link ElasticQuery#searchAfter(String)}.
     */
    public static final Mapping ID_FIELD_MAPPING = Mapping.named(ID_FIELD);

    private static final int DEFAULT_HTTP_PORT = 9200;

    private static final String SERVICE_ELASTICSEARCH = "elasticsearch";
    private static final String SCHEME_HTTP = "http";

    @Part
    @Nullable
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

    /**
     * Determines if the effective routing should be computed for a read or an write access.
     * <p>
     * This needs to be specified as we support that the routing suppression of the read index is
     * different from the routing suppression of the write index. Thus one can migrate from an
     * unrouted index to a routed one and vice versa.
     */
    enum RoutingAccessMode {
        READ, WRITE
    }

    @ConfigValue("elasticsearch.suppressedRoutings")
    private List<String> suppressedRoutings;
    private final Map<EntityDescriptor, EnumSet<RoutingAccessMode>> suppressedRoutingsMap = new HashMap<>();
    private static final EnumSet<RoutingAccessMode> NO_SUPPRESSION = EnumSet.noneOf(RoutingAccessMode.class);

    private LowLevelClient client;

    protected Future readyFuture = new Future();
    protected Average callDuration = new Average();
    protected Counter numSlowQueries = new Counter();

    private final Map<EntityDescriptor, Property> routeTable = new HashMap<>();
    private final Map<EntityDescriptor, String> writeIndexTable = new ConcurrentHashMap<>();
    private boolean dockerDetected = false;

    protected void updateRouteTable(EntityDescriptor ed, Property p) {
        if (suppressedRoutings != null && suppressedRoutings.contains(ed.getRelationName())) {
            Elastic.LOG.INFO("Routing for %s (%s) is suppressed via 'elasticsearch.suppressedRoutings'...",
                             ed.getRelationName(),
                             ed.getType().getSimpleName());
            suppressedRoutingsMap.put(ed, EnumSet.of(RoutingAccessMode.READ, RoutingAccessMode.WRITE));
        }

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
                waitForElasticsearchToBecomeReady();
            }
        }
    }

    private void waitForElasticsearchToBecomeReady() {
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
    protected void createEntity(ElasticEntity entity, EntityDescriptor entityDescriptor) throws Exception {
        JSONObject data = new JSONObject();
        String id = determineId(entity);
        entity.setId(id);
        toJSON(entityDescriptor, entity, data);

        JSONObject response = getLowLevelClient().index(determineWriteAlias(entityDescriptor),
                                                        id,
                                                        determineRouting(entityDescriptor,
                                                                         entity,
                                                                         RoutingAccessMode.WRITE),
                                                        null,
                                                        null,
                                                        data);
        if (entityDescriptor.isVersioned()) {
            entity.setPrimaryTerm(response.getLong(RESPONSE_PRIMARY_TERM));
            entity.setSeqNo(response.getLong(RESPONSE_SEQ_NO));
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
    protected String determineRouting(EntityDescriptor ed, ElasticEntity entity, RoutingAccessMode accessMode) {
        if (isRoutingSuppressed(ed, accessMode)) {
            return null;
        }

        Property property = routeTable.get(ed);

        if (property == null) {
            return null;
        }

        return String.valueOf(property.getValueForDatasource(Elastic.class, entity));
    }

    @Override
    protected void updateEntity(ElasticEntity entity, boolean force, EntityDescriptor entityDescriptor)
            throws Exception {
        JSONObject data = new JSONObject();
        boolean changed = toJSON(entityDescriptor, entity, data);

        if (!changed) {
            return;
        }

        JSONObject response = getLowLevelClient().index(determineWriteAlias(entityDescriptor),
                                                        determineId(entity),
                                                        determineRouting(entityDescriptor,
                                                                         entity,
                                                                         RoutingAccessMode.WRITE),
                                                        determinePrimaryTerm(force, entityDescriptor, entity),
                                                        determineSeqNo(force, entityDescriptor, entity),
                                                        data);

        if (entityDescriptor.isVersioned()) {
            entity.setPrimaryTerm(response.getLong(RESPONSE_PRIMARY_TERM));
            entity.setSeqNo(response.getLong(RESPONSE_SEQ_NO));
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
            data.put(p.getPropertyName(), p.getValueForDatasource(Elastic.class, entity));
            changed |= ed.isChanged(entity, p);
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
     * Determines the alias for the currently active read index for the given {@link EntityDescriptor}.
     *
     * @param ed the descriptor of the entity to determine the read alias for
     * @return the alias of the currently active index
     */
    public String determineReadAlias(EntityDescriptor ed) {
        return ed.getRelationName() + ACTIVE_ALIAS;
    }

    /**
     * Determines the alias for the currently active write index for the given {@link EntityDescriptor}.
     *
     * @param ed the descriptor of the entity to determine the write alias for
     * @return the alias of the currently active index
     */
    protected String determineWriteAlias(EntityDescriptor ed) {
        return writeIndexTable.getOrDefault(ed, ed.getRelationName() + ACTIVE_ALIAS);
    }

    /**
     * Resolves the effective index name being used for the given entity.
     *
     * @param ed the descriptor of the entity to determine the effective index for
     * @return the effective index to which {@link #determineReadAlias(EntityDescriptor) the read alias} points
     * @throws sirius.kernel.health.HandledException in case the alias setup failed and the expected alias does
     *                                               not exist
     */
    public String determineEffectiveIndex(EntityDescriptor ed) {
        return getLowLevelClient().resolveIndexForAlias(determineReadAlias(ed))
                                  .orElseThrow(() -> Exceptions.handle()
                                                               .to(Elastic.LOG)
                                                               .withSystemErrorMessage(
                                                                       "There is no index present for the alias (%s) of entity '%s'",
                                                                       determineReadAlias(ed),
                                                                       ed.getType().getName())
                                                               .handle());
    }

    /**
     * Creates a new write index for the given entity and installs it into the {@link #writeIndexTable}.
     * <p>
     * This will also install the most current mappings into the newly created index.
     *
     * @param ed the entity descriptor of the entity to install a new write index for
     */
    public void createAndInstallWriteIndex(EntityDescriptor ed) {
        String nextIndexName = indexMappings.determineNextIndexName(ed);
        indexMappings.createMapping(ed, nextIndexName, IndexMappings.DynamicMapping.STRICT);
        installWriteIndex(ed, nextIndexName);
    }

    /**
     * Installs the given write index for the given entity by writing it into the {@link #writeIndexTable}.
     * <p>
     * This can be used if the write index has already been created previously or on another node.
     *
     * @param ed            the descriptor of the entity to set the write index for
     * @param nextIndexName the index name to send writes to (This is probably a name like entity-2021-01-01).
     */
    public void installWriteIndex(EntityDescriptor ed, String nextIndexName) {
        writeIndexTable.put(ed, nextIndexName);
    }

    /**
     * Makes the current write index of the given entity also the read index by moving the {@link #ACTIVE_ALIAS}.
     * <p>
     * This will also remove the write-redirection by clearing the entry in the {@link #writeIndexTable} as
     * the indices / aliases are now the same.
     *
     * @param ed the entity descriptor for which the write index should also become the new read index / alias
     */
    public void commitWriteIndex(EntityDescriptor ed) {
        String writeIndexName = writeIndexTable.get(ed);
        if (writeIndexName == null) {
            throw Exceptions.createHandled()
                            .withSystemErrorMessage("These is no write index available for %s", ed.getType().getName())
                            .handle();
        }

        getLowLevelClient().createOrMoveAlias(determineReadAlias(ed), writeIndexName).toJSONString();
        writeIndexTable.remove(ed);
    }

    /**
     * Clears the current write index of the given entity.
     * <p>
     * Note that the underlying index will <b>NOT</b> be deleted. The {@link ESIndexCommand} can be used to achieve this.
     *
     * @param ed the entity descriptor for which the write index should be cleared
     */
    public void rollbackWriteIndex(EntityDescriptor ed) {
        writeIndexTable.remove(ed);
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
     * Determines the primary term to use for a given entity.
     *
     * @param force  <tt>true</tt> if an update should be forced
     * @param entity the entity to determine the primary term from
     * @return <tt>null</tt> if an update is forced or if the entity isn't
     * {@link sirius.db.mixing.annotations.Versioned}, the actual primary term otherwise.
     */
    private Long determinePrimaryTerm(boolean force, EntityDescriptor ed, ElasticEntity entity) {
        if (ed.isVersioned() && !force) {
            return entity.getPrimaryTerm();
        }

        return null;
    }

    /**
     * Determines the sequence number to use for a given entity.
     *
     * @param force  <tt>true</tt> if an update should be forced
     * @param entity the entity to determine the sequence number from
     * @return <tt>null</tt> if an update is forced or if the entity isn't
     * {@link sirius.db.mixing.annotations.Versioned}, the actual sequence number otherwise.
     */
    private Long determineSeqNo(boolean force, EntityDescriptor ed, ElasticEntity entity) {
        if (ed.isVersioned() && !force) {
            return entity.getSeqNo();
        }

        return null;
    }

    @Override
    protected void deleteEntity(ElasticEntity entity, boolean force, EntityDescriptor entityDescriptor)
            throws Exception {
        getLowLevelClient().delete(determineWriteAlias(entityDescriptor),
                                   entity.getId(),
                                   determineRouting(entityDescriptor, entity, RoutingAccessMode.WRITE),
                                   determinePrimaryTerm(force, entityDescriptor, entity),
                                   determineSeqNo(force, entityDescriptor, entity));
    }

    /**
     * Creates a new instance of the given entity type for the given data.
     *
     * @param ed  the descriptor of the entity type
     * @param obj the JSON data to transform
     * @return a new entity based on the given data
     */
    protected static ElasticEntity make(EntityDescriptor ed, JSONObject obj) {
        String id = obj.getString(ID_FIELD);

        try {
            JSONObject source = obj.getJSONObject(RESPONSE_SOURCE);
            ElasticEntity result = (ElasticEntity) ed.make(Elastic.class, null, key -> Value.of(source.get(key)));
            result.setSearchHit(obj);
            result.setId(id);

            if (ed.isVersioned()) {
                result.setPrimaryTerm(obj.getLong(RESPONSE_PRIMARY_TERM));
                result.setSeqNo(obj.getLong(RESPONSE_SEQ_NO));
            }

            return result;
        } catch (Exception e) {
            throw Exceptions.handle()
                            .error(e)
                            .withSystemErrorMessage("Failed processing entity (_id = %s)", id)
                            .to(Elastic.LOG)
                            .handle();
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
                                                               EntityDescriptor entityDescriptor,
                                                               Function<String, Value> context) throws Exception {
        JSONObject obj = getLowLevelClient().get(determineReadAlias(entityDescriptor),
                                                 id.toString(),
                                                 determineRoutingForFind(id, entityDescriptor, context),
                                                 true);
        if (obj == null || !Boolean.TRUE.equals(obj.getBoolean(RESPONSE_FOUND))) {
            return Optional.empty();
        }

        E result = (E) make(entityDescriptor, obj);
        return Optional.of(result);
    }

    private String determineRoutingForFind(Object id,
                                           EntityDescriptor entityDescriptor,
                                           Function<String, Value> context) {
        if (isRoutingSuppressed(entityDescriptor, RoutingAccessMode.READ)) {
            return null;
        }

        String routing = context.apply(CONTEXT_ROUTING).getString();
        if (routing == null && isRouted(entityDescriptor, RoutingAccessMode.READ)) {
            LOG.WARN("Trying to FIND an entity of type '%s' with id '%s' without providing a routing! "
                     + "This will most probably return an invalid result!\n%s",
                     entityDescriptor.getType().getName(),
                     id,
                     ExecutionPoint.snapshot());
        } else if (routing != null && !isRouted(entityDescriptor, RoutingAccessMode.READ)) {
            LOG.WARN("Trying to FIND an unrouted entity of type '%s' with id '%s' with a routing! "
                     + "This will most probably return an invalid result!\n%s",
                     entityDescriptor.getType().getName(),
                     id,
                     ExecutionPoint.snapshot());
        }

        return routing;
    }

    /**
     * Determines if the entity of the given descriptor requires a routing value.
     *
     * @param entityDescriptor the descriptor of the entity to check
     * @param accessMode       the access mode for which the routing should be checked
     * @return <tt>true</tt> if a routing is required, <tt>false</tt> otherwise
     */
    protected boolean isRouted(EntityDescriptor entityDescriptor, RoutingAccessMode accessMode) {
        return !isRoutingSuppressed(entityDescriptor, accessMode) && routeTable.containsKey(entityDescriptor);
    }

    /**
     * Determines if the usage of the routing for the given descriptor has been suppressed.
     * <p>
     * Via the config list {@link #suppressedRoutings} (<tt>elasticsearch.suppressedRoutings</tt>) the usage of
     * a routing field can entirely be disabled for all listed entities. This might be useful when migrating
     * from an unrouted to a routed index or even if the routing is only feasible in some scenarios.
     *
     * @param entityDescriptor the descriptor of the entity to check
     * @param accessMode       the access mode for which the suppression should be checked
     * @return <tt>true</tt> if routing for this descriptor has been explicitly suppressed, <tt>false</tt> otherwise
     */
    protected boolean isRoutingSuppressed(EntityDescriptor entityDescriptor, RoutingAccessMode accessMode) {
        return suppressedRoutingsMap.getOrDefault(entityDescriptor, NO_SUPPRESSION).contains(accessMode);
    }

    protected void updateRoutingSuppression(EntityDescriptor entityDescriptor,
                                            @Nullable EnumSet<RoutingAccessMode> modes) {
        if (modes == null) {
            if (suppressedRoutings.contains(entityDescriptor.getRelationName())) {
                modes = EnumSet.of(RoutingAccessMode.READ, RoutingAccessMode.WRITE);
            } else {
                modes = NO_SUPPRESSION;
            }
        }

        if (modes.isEmpty()) {
            suppressedRoutingsMap.remove(entityDescriptor);
        } else {
            suppressedRoutingsMap.put(entityDescriptor, modes);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <E extends ElasticEntity> Optional<E> findEntity(E entity) {
        return find((Class<E>) entity.getClass(),
                    entity.getId(),
                    routedBy(determineRouting(entity.getDescriptor(), entity, RoutingAccessMode.READ)));
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
     * @return the log threshold for queries in milliseconds
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
     * Creates a new suggestion query.
     * <p>
     * A suggestion query can be used to generate term or phrase suggestions for a given input text.
     *
     * @param type the target entity for which suggestions should be computed
     * @param <E>  the generic type of the entity
     * @return a new suggestion query for the given type
     */
    public <E extends ElasticEntity> SuggestionQuery<E> suggest(Class<E> type) {
        return new SuggestionQuery<>(mixing.getDescriptor(type), getLowLevelClient());
    }

    /**
     * Allows to explicitly refresh the index for the given {@link ElasticEntity}, making all operations performed
     * since the last refresh available for search.
     *
     * @param type the entity type which should be refreshed
     * @param <E>  the concrete type which should be refreshed
     */
    public <E extends ElasticEntity> void refresh(Class<E> type) {
        getLowLevelClient().refresh(determineWriteAlias(mixing.getDescriptor(type)));
    }

    @Override
    public ElasticFilterFactory filters() {
        return FILTERS;
    }

    @Override
    public Value fetchField(Class<? extends ElasticEntity> type, Object id, Mapping field) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected int determineRetryTimeoutFactor() {
        return 500;
    }

    /**
     * Create a shallow copy of the given JSON object.
     *
     * @param json the object to copy
     * @return a shallow copy of the given JSON object
     */
    @SuppressWarnings("java:S1168")
    @Explain("We don't really return a map or collection here, so null is more expected than an empty json object")
    public static JSONObject copyJSON(JSONObject json) {
        if (json == null) {
            return null;
        }

        return json.clone();
    }
}
