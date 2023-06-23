/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.fasterxml.jackson.databind.node.ObjectNode;
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
import sirius.kernel.commons.Json;
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
    private static final String ID_FIELD = "_id";

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
     * Determines if the effective routing should be computed for a read or a write access.
     * <p>
     * This needs to be specified as we support that the routing suppression of the read index is
     * different from the routing suppression of the write index. Thus, one can migrate from an
     * un-routed index to a routed one and vice versa.
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

    protected void updateRouteTable(EntityDescriptor entityDescriptor, Property property) {
        if (suppressedRoutings != null && suppressedRoutings.contains(entityDescriptor.getRelationName())) {
            Elastic.LOG.INFO("Routing for %s (%s) is suppressed via 'elasticsearch.suppressedRoutings'...",
                             entityDescriptor.getRelationName(),
                             entityDescriptor.getType().getSimpleName());
            suppressedRoutingsMap.put(entityDescriptor, EnumSet.of(RoutingAccessMode.READ, RoutingAccessMode.WRITE));
        }

        routeTable.put(entityDescriptor, property);
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
            // to fully boot up. Otherwise, strange connection issues might arise.
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
            } catch (Exception exception) {
                Exceptions.ignore(exception);
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
            } catch (NumberFormatException exception) {
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
        ObjectNode data = Json.createObject();
        String id = determineId(entity);
        entity.setId(id);
        toJSON(entityDescriptor, entity, data);

        ObjectNode response = getLowLevelClient().index(determineWriteAlias(entityDescriptor),
                                                        id,
                                                        determineRouting(entityDescriptor,
                                                                         entity,
                                                                         RoutingAccessMode.WRITE),
                                                        null,
                                                        null,
                                                        data);
        if (entityDescriptor.isVersioned()) {
            entity.setPrimaryTerm(response.required(RESPONSE_PRIMARY_TERM).asLong());
            entity.setSeqNo(response.required(RESPONSE_SEQ_NO).asLong());
        }
    }

    /**
     * Determines the routing value to be used for the given entity.
     *
     * @param entityDescriptor the entity descriptor of the entity
     * @param entity           the entity to fetch the routing value from
     * @return the routing value to use
     */
    @Nullable
    protected String determineRouting(EntityDescriptor entityDescriptor,
                                      ElasticEntity entity,
                                      RoutingAccessMode accessMode) {
        if (isRoutingSuppressed(entityDescriptor, accessMode)) {
            return null;
        }

        Property property = routeTable.get(entityDescriptor);

        if (property == null) {
            return null;
        }

        return String.valueOf(property.getValueForDatasource(Elastic.class, entity));
    }

    @Override
    protected void updateEntity(ElasticEntity entity, boolean force, EntityDescriptor entityDescriptor)
            throws Exception {
        ObjectNode data = Json.createObject();
        boolean changed = toJSON(entityDescriptor, entity, data);

        if (!changed) {
            return;
        }

        ObjectNode response = getLowLevelClient().index(determineWriteAlias(entityDescriptor),
                                                        determineId(entity),
                                                        determineRouting(entityDescriptor,
                                                                         entity,
                                                                         RoutingAccessMode.WRITE),
                                                        determinePrimaryTerm(force, entityDescriptor, entity),
                                                        determineSeqNo(force, entityDescriptor, entity),
                                                        data);

        if (entityDescriptor.isVersioned()) {
            entity.setPrimaryTerm(response.required(RESPONSE_PRIMARY_TERM).asLong());
            entity.setSeqNo(response.required(RESPONSE_SEQ_NO).asLong());
        }
    }

    /**
     * Transforms the given entity to JSON.
     *
     * @param entity           the entity to transform
     * @param entityDescriptor the descriptor of the entity
     * @param data             the target JSON to fill
     * @return <tt>true</tt> if at least on property has changed, <tt>false</tt> otherwise
     */
    protected boolean toJSON(EntityDescriptor entityDescriptor, ElasticEntity entity, ObjectNode data) {
        boolean changed = false;
        for (Property property : entityDescriptor.getProperties()) {
            data.putPOJO(property.getPropertyName(), property.getValueForDatasource(Elastic.class, entity));
            changed |= entityDescriptor.isChanged(entity, property);
        }
        return changed;
    }

    /**
     * Determines the ID of the entity.
     * <p>
     * This will either return the stored ID or create a new one, if the entity is still new.
     *
     * @param entity the entity to determine the ID for
     * @return the ID to use for this entity
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
     * @param entityDescriptor the descriptor of the entity
     * @return the index name to use for the given entity.
     */
    protected String determineIndex(EntityDescriptor entityDescriptor) {
        if (indexNaming == null) {
            return entityDescriptor.getRelationName();
        }

        return indexNaming.determineIndexName(entityDescriptor);
    }

    /**
     * Determines the alias for the currently active read index for the given {@link EntityDescriptor}.
     *
     * @param entityDescriptor the descriptor of the entity to determine the read alias for
     * @return the alias of the currently active index
     */
    public String determineReadAlias(EntityDescriptor entityDescriptor) {
        return entityDescriptor.getRelationName() + ACTIVE_ALIAS;
    }

    /**
     * Determines the alias for the currently active write index for the given {@link EntityDescriptor}.
     *
     * @param entityDescriptor the descriptor of the entity to determine the write alias for
     * @return the alias of the currently active index
     */
    protected String determineWriteAlias(EntityDescriptor entityDescriptor) {
        return writeIndexTable.getOrDefault(entityDescriptor, entityDescriptor.getRelationName() + ACTIVE_ALIAS);
    }

    /**
     * Resolves the effective index name being used for the given entity.
     *
     * @param entityDescriptor the descriptor of the entity to determine the effective index for
     * @return the effective index to which {@link #determineReadAlias(EntityDescriptor) the read alias} points
     * @throws sirius.kernel.health.HandledException in case the alias setup failed and the expected alias does
     *                                               not exist
     */
    public String determineEffectiveIndex(EntityDescriptor entityDescriptor) {
        return getLowLevelClient().resolveIndexForAlias(determineReadAlias(entityDescriptor))
                                  .orElseThrow(() -> Exceptions.handle()
                                                               .to(Elastic.LOG)
                                                               .withSystemErrorMessage(
                                                                       "There is no index present for the alias (%s) of entity '%s'",
                                                                       determineReadAlias(entityDescriptor),
                                                                       entityDescriptor.getType().getName())
                                                               .handle());
    }

    /**
     * Creates a new write index for the given entity and installs it into the {@link #writeIndexTable}.
     * <p>
     * This will also install the most current mappings into the newly created index.
     *
     * @param entityDescriptor the entity descriptor of the entity to install a new write index for
     */
    public void createAndInstallWriteIndex(EntityDescriptor entityDescriptor) {
        String nextIndexName = indexMappings.determineNextIndexName(entityDescriptor);
        indexMappings.createMapping(entityDescriptor, nextIndexName, IndexMappings.DynamicMapping.STRICT);
        installWriteIndex(entityDescriptor, nextIndexName);
    }

    /**
     * Installs the given write index for the given entity by writing it into the {@link #writeIndexTable}.
     * <p>
     * This can be used if the write index has already been created previously or on another node.
     *
     * @param entityDescriptor the descriptor of the entity to set the write index for
     * @param nextIndexName    the index name to send writes to (This is probably a name like entity-2021-01-01).
     */
    public void installWriteIndex(EntityDescriptor entityDescriptor, String nextIndexName) {
        writeIndexTable.put(entityDescriptor, nextIndexName);
    }

    /**
     * Makes the current write index of the given entity also the read index by moving the {@link #ACTIVE_ALIAS}.
     * <p>
     * This will also remove the write-redirection by clearing the entry in the {@link #writeIndexTable} as
     * the indices / aliases are now the same.
     *
     * @param entityDescriptor the entity descriptor for which the write index should also become the new read index / alias
     */
    public void commitWriteIndex(EntityDescriptor entityDescriptor) {
        String writeIndexName = writeIndexTable.get(entityDescriptor);
        if (writeIndexName == null) {
            throw Exceptions.createHandled()
                            .withSystemErrorMessage("These is no write index available for %s",
                                                    entityDescriptor.getType().getName())
                            .handle();
        }

        Json.write(getLowLevelClient().createOrMoveAlias(determineReadAlias(entityDescriptor), writeIndexName));
        writeIndexTable.remove(entityDescriptor);
    }

    /**
     * Clears the current write index of the given entity.
     * <p>
     * Note that the underlying index will <b>NOT</b> be deleted. The {@link ESIndexCommand} can be used to achieve this.
     *
     * @param entityDescriptor the entity descriptor for which the write index should be cleared
     */
    public void rollbackWriteIndex(EntityDescriptor entityDescriptor) {
        writeIndexTable.remove(entityDescriptor);
    }

    /**
     * Determines the type name used for a given entity type.
     * <p>
     * It will be determined by {@link IndexNaming} if it is implemented, or it will equal the lowercase
     * {@link Class#getSimpleName() name of the entity}.
     *
     * @param entityDescriptor the descriptor of the entity
     * @return the type name to use
     */
    protected String determineTypeName(EntityDescriptor entityDescriptor) {
        if (indexNaming == null) {
            return entityDescriptor.getType().getSimpleName().toLowerCase();
        }

        return indexNaming.determineMappingName(entityDescriptor);
    }

    /**
     * Determines the primary term to use for a given entity.
     *
     * @param force  <tt>true</tt> if an update should be forced
     * @param entity the entity to determine the primary term from
     * @return <tt>null</tt> if an update is forced or if the entity isn't
     * {@link sirius.db.mixing.annotations.Versioned}, the actual primary term otherwise.
     */
    private Long determinePrimaryTerm(boolean force, EntityDescriptor entityDescriptor, ElasticEntity entity) {
        if (entityDescriptor.isVersioned() && !force) {
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
    private Long determineSeqNo(boolean force, EntityDescriptor entityDescriptor, ElasticEntity entity) {
        if (entityDescriptor.isVersioned() && !force) {
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
     * @param entityDescriptor the descriptor of the entity type
     * @param data             the JSON data to transform
     * @return a new entity based on the given data
     */
    protected static ElasticEntity make(EntityDescriptor entityDescriptor, ObjectNode data) {
        String id = Json.tryValueString(data, ID_FIELD).orElse(null);

        try {
            ObjectNode source = Json.getObject(data, RESPONSE_SOURCE);
            ElasticEntity result = (ElasticEntity) entityDescriptor.make(Elastic.class,
                                                                         null,
                                                                         key -> Json.convertToValue(source.get(key)));
            result.setSearchHit(data);
            result.setId(id);

            if (entityDescriptor.isVersioned()) {
                result.setPrimaryTerm(data.required(RESPONSE_PRIMARY_TERM).asLong());
                result.setSeqNo(data.required(RESPONSE_SEQ_NO).asLong());
            }

            return result;
        } catch (Exception exception) {
            throw Exceptions.handle()
                            .error(exception)
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
        ObjectNode jsonEntity = getLowLevelClient().get(determineReadAlias(entityDescriptor),
                                                        id.toString(),
                                                        determineRoutingForFind(id, entityDescriptor, context),
                                                        true);
        if (jsonEntity == null || !jsonEntity.path(RESPONSE_FOUND).asBoolean()) {
            return Optional.empty();
        }

        E result = (E) make(entityDescriptor, jsonEntity);
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
            LOG.WARN("Trying to FIND an entity of type '%s' with ID '%s' without providing a routing! "
                     + "This will most probably return an invalid result!\n%s",
                     entityDescriptor.getType().getName(),
                     id,
                     ExecutionPoint.snapshot());
        } else if (routing != null && !isRouted(entityDescriptor, RoutingAccessMode.READ)) {
            LOG.WARN("Trying to FIND an un-routed entity of type '%s' with ID '%s' with a routing! "
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
     * from an un-routed to a routed index or even if the routing is only feasible in some scenarios.
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
     * @param refresh the refresh mode to use when submitting a bulk update
     * @return a new batch context
     */
    public BulkContext batch(LowLevelClient.Refresh refresh) {
        return new BulkContext(getLowLevelClient(), refresh);
    }

    /**
     * Creates a {@link BulkContext batch context} used for bulk updates.
     *
     * @return a new batch context
     */
    public BulkContext batch() {
        return batch(LowLevelClient.Refresh.FALSE);
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
     * Creates a new query which can be used to fetch entities of the given types.
     * <p>
     * Note: All entities to query across must extend a common super class. But also note, that the
     * <tt>commonSuperClass</tt> itself is not queried (as it might be an abstract type). All indices to query must
     * be listed in <tt>type</tt>.
     *
     * @param commonSuperClass the common super class of all entities to query across
     * @param types            the types of the entities to query across
     * @param <E>              the generic common type of the entities to query across
     * @return a new query which can be used to fetch entities of the given types
     */
    @SuppressWarnings("java:S1172")
    @Explain("We only need this parameter to make the compiler enforce proper type rules.")
    @SafeVarargs
    public final <E extends ElasticEntity> ElasticQuery<E> selectMultiple(Class<E> commonSuperClass,
                                                                          Class<? extends E>... types) {
        return selectMultiple(commonSuperClass, Arrays.asList(types));
    }

    /**
     * Creates a new query which can be used to fetch entities of the given types.
     * <p>
     * Note: All entities to query across must extend a common super class. But also note, that the
     * <tt>commonSuperClass</tt> itself is not queried (as it might be an abstract type). All indices to query must
     * be listed in <tt>type</tt>.
     *
     * @param commonSuperClass the common super class of all entities to query across
     * @param types            the types of the entities to query across
     * @param <E>              the generic common type of the entities to query across
     * @return a new query which can be used to fetch entities of the given types
     */
    @SuppressWarnings("java:S1172")
    @Explain("We only need this parameter to make the compiler enforce proper type rules.")
    public final <E extends ElasticEntity> ElasticQuery<E> selectMultiple(Class<E> commonSuperClass,
                                                                          List<Class<? extends E>> types) {
        return new ElasticQuery<E>(mixing.getDescriptor(types.get(0)),
                                   getLowLevelClient()).withAdditionalIndices(types.stream().skip(1));
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
    public static ObjectNode copyJSON(ObjectNode json) {
        if (json == null) {
            return null;
        }

        return Json.clone(json);
    }
}
