/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import sirius.db.es.constraints.BoolQueryBuilder;
import sirius.db.es.constraints.ElasticConstraint;
import sirius.db.mixing.DateRange;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.OptimisticLockException;
import sirius.db.mixing.query.Query;
import sirius.db.mixing.query.constraints.FilterFactory;
import sirius.kernel.async.ExecutionPoint;
import sirius.kernel.async.TaskContext;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Json;
import sirius.kernel.commons.PullBasedSpliterator;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Provides a fluent query API for Elasticsearch.
 *
 * @param <E> the type of entities to be queried
 */
public class ElasticQuery<E extends ElasticEntity> extends Query<ElasticQuery<E>, E, ElasticConstraint> {
    /**
     * If we only fetch from a single shard (as we use a routing), we fetch up to {@link #BLOCK_SIZE_FOR_SINGLE_SHARD}
     * entities at once and hope to process them within {@link #STREAM_BLOCKWISE_PIT_TTL}.
     */
    private static final int BLOCK_SIZE_FOR_SINGLE_SHARD = 800;

    /**
     * If we fetch from many shards, we fetch up to {@link #BLOCK_SIZE_PER_SHARD} entities per shards and hope to
     * process them within {@link #STREAM_BLOCKWISE_PIT_TTL}.
     */
    private static final int BLOCK_SIZE_PER_SHARD = 100;

    public static final String SHARD_DOC_ID = "_shard_doc";

    private static final String KEY_FIELD = "field";
    private static final String KEY_SIZE = "size";
    private static final String KEY_DATE_RANGE = "date_range";
    private static final String KEY_KEYED = "keyed";
    private static final String KEY_RANGES = "ranges";
    private static final String KEY_QUERY = "query";
    private static final String KEY_SORT = "sort";
    private static final String KEY_SEARCH_AFTER = "search_after";
    private static final String KEY_AGGS = "aggs";
    private static final String KEY_POST_FILTER = "post_filter";
    private static final String KEY_INNER_HITS = "inner_hits";
    private static final String KEY_COLLAPSE = "collapse";
    private static final String KEY_COUNT = "count";
    private static final String KEY_HITS = "hits";
    private static final String KEY_TOTAL = "total";
    private static final String KEY_AGGREGATIONS = "aggregations";
    private static final String KEY_KEY = "key";
    private static final String KEY_NAME = "name";
    private static final String KEY_ASC = "asc";
    private static final String KEY_DESC = "desc";
    private static final String KEY_ORDER = "order";
    private static final String KEY_FROM = "from";
    private static final String KEY_TO = "to";
    private static final String KEY_EXPLAIN = "explain";
    private static final String KEY_SUGGEST = "suggest";
    private static final String KEY_VALUE = "value";
    private static final String KEY_SEQ_NO_PRIMARY_TERM = "seq_no_primary_term";
    private static final String KEY_TIMED_OUT = "timed_out";
    private static final String KEY_VERSION_CONFLICTS = "version_conflicts";
    private static final String KEY_FAILURES = "failures";
    private static final Mapping SCORE = Mapping.named("_score");
    /**
     * The elastic PIT timeout to use with streamBlockwise.
     * <p>
     * This timeout / TTL keeps the PIT open. Using a high time is not a problem here, because it gets closed after the
     * stream is consumed, so we rather try to choose a high timeout to avoid issues with prematurely closed PITs.
     */
    private static final String STREAM_BLOCKWISE_PIT_TTL = "30m";
    private static final String KEY_PIT = "pit";
    private static final String KEY_PIT_ID = "id";
    private static final String KEY_PIT_KEEP_ALIVE = "keep_alive";

    @Part
    private static Elastic elastic;

    @Part
    private static IndexMappings indexMappings;

    /**
     * Contains a list of additional descriptors / entities to perform a query across multiple indices.
     */
    protected List<EntityDescriptor> additionalDescriptors;

    private final LowLevelClient client;

    private BoolQueryBuilder queryBuilder;

    private List<AggregationBuilder> aggregations;

    private BoolQueryBuilder postFilters;

    private String collapseBy;
    private List<ObjectNode> collapseByInnerHits;

    private List<ObjectNode> sorts;

    private List<String> searchAfter;

    private FunctionScoreBuilder functionScore;

    private NearestNeighborsSearch nearestNeighborsSearch;

    private String routing;
    private boolean unrouted;

    private boolean explain;

    private Map<String, ObjectNode> suggesters;

    private ObjectNode response;

    /**
     * Used to describe inner hits which are determined for field collapsing.
     * <p>
     * Note that there is no <tt>build()</tt> method, as the constructor already applies the object to the query.
     */
    public class InnerHitsBuilder {
        private final ObjectNode json;
        private List<ObjectNode> sorts;

        protected InnerHitsBuilder(String name, int size) {
            this.json = Json.createObject().put(KEY_NAME, name).put(KEY_SIZE, size);
            ElasticQuery.this.collapseByInnerHits = autoinit(ElasticQuery.this.collapseByInnerHits);
            ElasticQuery.this.collapseByInnerHits.add(json);
        }

        /**
         * Adds a sorting criterion for the given field, in ascending order.
         *
         * @param field the field to sort by
         * @return the builder itself for fluent method calls
         */
        public InnerHitsBuilder orderByAsc(String field) {
            if (this.sorts == null) {
                this.sorts = new ArrayList<>();
                this.json.putPOJO(KEY_SORT, sorts);
            }
            this.sorts.add(Json.createObject().put(field, KEY_ASC));
            return this;
        }

        /**
         * Adds a sorting criterion for the given field, in ascending order.
         *
         * @param field the field to sort by
         * @return the builder itself for fluent method calls
         */
        public InnerHitsBuilder orderByAsc(Mapping field) {
            return orderByAsc(field.getName());
        }

        /**
         * Adds a sorting criterion for the given field, in descending order.
         *
         * @param field the field to sort by
         * @return the builder itself for fluent method calls
         */
        public InnerHitsBuilder orderByDesc(String field) {
            if (this.sorts == null) {
                this.sorts = new ArrayList<>();
                this.json.putPOJO(KEY_SORT, sorts);
            }
            this.sorts.add(Json.createObject().put(field, KEY_DESC));
            return this;
        }

        /**
         * Adds a parameter to the inner hit.
         *
         * @param name  the name of the parameter
         * @param value the value of the parameter
         * @return the builder itself for fluent method calls
         */
        public InnerHitsBuilder addParameter(String name, Object value) {
            this.json.putPOJO(name, value);
            return this;
        }
    }

    /**
     * Creates a new query for the given type using the given client.
     *
     * @param descriptor the descriptor of the entity type to query
     * @param client     the client to use
     * @see Elastic#select(Class)
     */
    public ElasticQuery(EntityDescriptor descriptor, LowLevelClient client) {
        super(descriptor);
        this.client = client;
    }

    /**
     * Creates a copy of this query.
     * <p>
     * Note that this query will inherit all filters, constraints, sorts and aggregations. After
     * the copy has been performed, both queries can be used and modified independently. However,
     * note that we perform a shallow copy. Therefore, if a query is e.g. supplier with an inner hits
     * builder (via {@link #addCollapsedInnerHits(String, int)}) and then copied, the builder will
     * be shared internally and modifying it, will affect both queries - therefore modifications like
     * this have to happen after a copy.
     * <p>
     * Also note that neither result hits nor result aggregations will be copied.
     *
     * @return a copy of this query.
     */
    public ElasticQuery<E> copy() {
        ElasticQuery<E> copy = new ElasticQuery<>(descriptor, client);
        copy.limit = this.limit;
        copy.skip = this.skip;
        copy.forceFail = this.forceFail;
        copy.routing = this.routing;
        copy.unrouted = this.unrouted;
        copy.explain = this.explain;
        copy.collapseBy = this.collapseBy;

        if (queryBuilder != null) {
            copy.queryBuilder = this.queryBuilder.copy();
        }

        if (aggregations != null) {
            copy.aggregations = this.aggregations.stream().map(AggregationBuilder::copy).collect(Collectors.toList());
        }

        if (postFilters != null) {
            copy.postFilters = this.postFilters.copy();
        }

        if (collapseByInnerHits != null) {
            copy.collapseByInnerHits =
                    this.collapseByInnerHits.stream().map(Elastic::copyJSON).collect(Collectors.toList());
        }

        if (sorts != null) {
            copy.sorts = this.sorts.stream().map(Elastic::copyJSON).collect(Collectors.toList());
        }

        if (searchAfter != null) {
            copy.searchAfter = new ArrayList<>(searchAfter);
        }

        if (functionScore != null) {
            copy.functionScore = this.functionScore.copy();
        }

        if (suggesters != null) {
            copy.suggesters = this.suggesters.entrySet()
                                             .stream()
                                             .collect(Collectors.toMap(Map.Entry::getKey,
                                                                       entry -> Json.clone(entry.getValue())));
        }

        copy.additionalDescriptors = additionalDescriptors;

        if (nearestNeighborsSearch != null) {
            copy.nearestNeighborsSearch = nearestNeighborsSearch.copy();
        }

        return copy;
    }

    private <X> List<X> autoinit(List<X> list) {
        if (list == null) {
            return new ArrayList<>();
        }

        return list;
    }

    /**
     * Spans the query over the given additional indices.
     * <p>
     * Note that the given indices are added to the main index / descriptor which is already present. Also, not
     * that all settings (most notably routing) are determined by looking at the main descriptor. Therefore, all
     * additional descriptors must share the same settings. Also note, that all entities / descriptors must share
     * the fields being queried / aggregated for this to make sense.
     *
     * @param additionalEntitiesToQuery the additional entities to be queried
     * @return the query itself for fluent method calls
     */
    @SafeVarargs
    public final ElasticQuery<E> withAdditionalIndices(Class<? extends ElasticEntity>... additionalEntitiesToQuery) {
        this.additionalDescriptors =
                Arrays.stream(additionalEntitiesToQuery).map(type -> mixing.getDescriptor(type)).toList();

        return this;
    }

    /**
     * Enables the explain mode which gives detailed information about score calculations.
     * <p>
     * Only use this mode for debugging as this might cost performance!
     *
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> explain() {
        this.explain = true;
        return this;
    }

    /**
     * Specifies a list of "last sort values" to continue a previous query.
     * <p>
     * Use {@link #getLastSortValues()} to obtain the proper values.
     *
     * @param searchAfter the last sort values of the previous result
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> searchAfter(List<String> searchAfter) {
        if (searchAfter != null) {
            this.searchAfter = new ArrayList<>(searchAfter);
        }

        return this;
    }

    /**
     * Sets a singular "last sort value" to continue a previous query.
     * <p>
     * It is recommended to sort by {@link ElasticEntity#ID} when using this. Note that this gracefully
     * handles empty values as well as "-" which is properly generated by {@link #getLastSortValue()} if no more
     * results are expected.
     *
     * @param searchAfter the last sort value of the previous result
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> searchAfter(String searchAfter) {
        if (Strings.isFilled(searchAfter)) {
            if ("-".equals(searchAfter)) {
                return this.fail();
            }

            this.searchAfter = Collections.singletonList(searchAfter);
        }

        return this;
    }

    /**
     * Obtains the list of last sort values to be used with {@link #searchAfter(List)}.
     *
     * @return the list of last sort values or an empty list if no results are present
     */
    public List<String> getLastSortValues() {
        if (response == null) {
            return Collections.emptyList();
        }

        ArrayNode jsonArray = getRawResponse().withArray(Strings.apply("/%s/%s", KEY_HITS, KEY_HITS));
        if (jsonArray == null || jsonArray.isEmpty()) {
            return Collections.emptyList();
        }

        ArrayNode sortArray = jsonArray.withArray(Strings.apply("/%d/%s", jsonArray.size() - 1, KEY_SORT));
        if (sortArray == null) {
            return Collections.emptyList();
        }

        return Json.streamEntries(sortArray).map(Object::toString).toList();
    }

    /**
     * Obtains the last sort value of this result.
     * <p>
     * Note that this will automatically return "-" if no more results are expected. Use this in conjunction with
     * {@link #searchAfter(String)} and sort queries by {@link ElasticEntity#ID}.
     *
     * @return the last sort value within this result
     */
    public String getLastSortValue() {
        if (limit > 0 && getRawResponse().withArray(Strings.apply("/%s/%s", KEY_HITS, KEY_HITS)).size() < limit) {
            return "-";
        }

        return getLastSortValues().stream().filter(Strings::isFilled).findFirst().orElse("-");
    }

    /**
     * Adds a MUST filter to the query.
     *
     * @param filter the filter to add
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> must(ObjectNode filter) {
        if (filter != null) {
            if (queryBuilder == null) {
                queryBuilder = new BoolQueryBuilder();
            }
            queryBuilder.must(filter);
        }
        return this;
    }

    /**
     * Adds a MUST filter to the query.
     *
     * @param filter the filter to add
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> must(ElasticConstraint filter) {
        if (filter != null) {
            must(filter.toJSON());
        }

        return this;
    }

    /**
     * Adds a MUST NOT filter to the query.
     *
     * @param filter the filter to add
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> mustNot(ObjectNode filter) {
        if (filter != null) {
            if (queryBuilder == null) {
                queryBuilder = new BoolQueryBuilder();
            }
            queryBuilder.mustNot(filter);
        }
        return this;
    }

    /**
     * Adds a MUST NOT filter to the query.
     *
     * @param filter the filter to add
     * @return the query itself for fluent method calls
     */

    public ElasticQuery<E> mustNot(ElasticConstraint filter) {
        if (filter != null) {
            mustNot(filter.toJSON());
        }

        return this;
    }

    /**
     * Adds a FILTER constraint to the query.
     *
     * @param filter the filter to add
     * @return the query itself for fluent method calls
     * @see BoolQueryBuilder#filter(ObjectNode)
     */
    public ElasticQuery<E> filter(ObjectNode filter) {
        if (filter != null) {
            if (queryBuilder == null) {
                queryBuilder = new BoolQueryBuilder();
            }
            queryBuilder.filter(filter);
        }
        return this;
    }

    @Override
    public ElasticQuery<E> where(ElasticConstraint constraint) {
        if (constraint != null) {
            return filter(constraint.toJSON());
        }

        return this;
    }

    /**
     * Executes a nearest neighbor search for a given vector field.
     *
     * @param nearestNeighborsSearch the search to execute
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> knn(NearestNeighborsSearch nearestNeighborsSearch) {
        this.nearestNeighborsSearch = nearestNeighborsSearch;

        return this;
    }

    /**
     * Adds a post filter to the query.
     *
     * @param filter the filter to add
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> postFilter(ObjectNode filter) {
        if (filter != null) {
            if (postFilters == null) {
                postFilters = new BoolQueryBuilder();
            }
            postFilters.filter(filter);
        }
        return this;
    }

    /**
     * Adds a post filter to the query.
     *
     * @param filter the filter to add
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> postFilter(ElasticConstraint filter) {
        if (filter != null) {
            postFilter(filter.toJSON());
        }

        return this;
    }

    /**
     * Permits to rewrite the internal filters of a query.
     * <p>
     * Actually this will iterate over all {@link BoolQueryBuilder#filter(ElasticConstraint)} of the internal query and
     * apply the given predicate. If this returns <tt>true</tt>, the filter will be supplied to the consumer and removed
     * internally.
     * <p>
     * This can e.g. be used to move internal filters into {@link #postFilter(ObjectNode)}.
     *
     * @param shouldRemove  the predicate to determine which filters to transform
     * @param removeHandler the callback to transform / process the filter
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> rewriteFilters(Predicate<ObjectNode> shouldRemove, Consumer<ObjectNode> removeHandler) {
        queryBuilder.removeFilterIf(constraint -> {
            if (shouldRemove.test(constraint)) {
                removeHandler.accept(constraint);
                return true;
            } else {
                return false;
            }
        });

        return this;
    }

    /**
     * Adds a sort statement to the query.
     *
     * @param sortBuilder a sort builder
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> sort(SortBuilder sortBuilder) {
        return sort(sortBuilder.build());
    }

    /**
     * Adds a sort statement to the query.
     *
     * @param sortSpec a JSON object describing a sort requirement
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> sort(ObjectNode sortSpec) {
        this.sorts = autoinit(this.sorts);
        sorts.add(sortSpec);
        return this;
    }

    /**
     * Adds a sort statement for the given field to the query.
     *
     * @param field    the field to sort by
     * @param sortSpec a JSON object describing a sort requirement
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> sort(Mapping field, ObjectNode sortSpec) {
        return sort(Json.createObject().set(field.toString(), sortSpec));
    }

    /**
     * Adds an ascending sort by the given field to the query.
     *
     * @param field the field to order by
     * @return the query itself for fluent method calls
     */
    @Override
    public ElasticQuery<E> orderAsc(Mapping field) {
        return sort(field, Json.createObject().put(KEY_ORDER, KEY_ASC));
    }

    /**
     * Adds a descending sort by the given field to the query.
     *
     * @param field the field to order by
     * @return the query itself for fluent method calls
     */
    @Override
    public ElasticQuery<E> orderDesc(Mapping field) {
        return sort(field, Json.createObject().put(KEY_ORDER, KEY_DESC));
    }

    /**
     * Adds the given function score to the query.
     *
     * @param functionScore the function score builder to use
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> functionScore(FunctionScoreBuilder functionScore) {
        this.functionScore = functionScore;
        return this;
    }

    /**
     * Adds an order by clause which sorts by <tt>_score</tt> ascending.
     *
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> orderByScoreAsc() {
        return orderAsc(SCORE);
    }

    /**
     * Adds an order by clause which sorts by <tt>_score</tt> descending.
     *
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> orderByScoreDesc() {
        return orderDesc(SCORE);
    }

    /**
     * Computes a random score and sorts by this.
     * <p>
     * Note that this will replace all other scoring.
     *
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> orderRandomly() {
        return functionScore(FunctionScoreBuilder.RANDOM_SCORE).orderByScoreDesc();
    }

    /**
     * Collapses by the given field.
     *
     * @param field the field to collapse results by.
     * @return the query itself for fluent method calls
     * @see #addCollapsedInnerHits(String, int)
     */
    public ElasticQuery<E> collapse(Mapping field) {
        this.collapseBy = field.toString();
        return this;
    }

    /**
     * Collapses by the given field.
     *
     * @param field the field to collapse results by.
     * @return the query itself for fluent method calls
     * @see #addCollapsedInnerHits(String, int)
     */
    public ElasticQuery<E> collapse(String field) {
        this.collapseBy = field;
        return this;
    }

    /**
     * Adds a description to obtain a sublist of collapsed results.
     *
     * @param name the name of the list
     * @param size the number of results
     * @return the builder which can be used to control sorting
     */
    public InnerHitsBuilder addCollapsedInnerHits(String name, int size) {
        return new InnerHitsBuilder(name, size);
    }

    /**
     * Adds a description to obtain a sublist of collapsed results.
     *
     * @param size the number of results
     * @return the builder which can be used to control sorting
     */
    public InnerHitsBuilder addCollapsedInnerHits(int size) {
        return new InnerHitsBuilder(ElasticEntity.DEFAULT_INNER_HITS, size);
    }

    /**
     * Adds the given aggregation for the given name.
     *
     * @param aggregation the aggregation itself
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> addAggregation(AggregationBuilder aggregation) {
        if (aggregations == null) {
            aggregations = new ArrayList<>();
        }

        aggregations.add(aggregation);

        return this;
    }

    /**
     * Clears all aggregations which have previously been added.
     *
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> clearAggregations() {
        this.aggregations = null;
        return this;
    }

    /**
     * Adds a term (bucket) aggregation for the given field.
     *
     * @param field the field to aggregate
     * @return the query itself for fluent method calls
     * @see #getAggregation(String)
     * @see AggregationResult#forEachBucket(Consumer)
     */
    public ElasticQuery<E> addTermAggregation(Mapping field) {
        return addTermAggregation(field.toString(), field, AggregationBuilder.DEFAULT_TERM_AGGREGATION_BUCKET_COUNT);
    }

    /**
     * Adds a term (bucket) aggregation for the given field.
     *
     * @param name  the name of the aggregation
     * @param field the field to aggregate
     * @param size  the max. number of buckets to return
     * @return the query itself for fluent method calls
     * @see #getAggregation(String)
     * @see AggregationResult#forEachBucket(Consumer)
     */
    public ElasticQuery<E> addTermAggregation(String name, Mapping field, int size) {
        return addAggregation(AggregationBuilder.create(AggregationBuilder.TERMS, name).field(field).size(size));
    }

    /**
     * Adds a cardinality aggregation for the given field.
     *
     * @param name  the name of the aggregation
     * @param field the field to aggregate
     * @return the query itself for fluent method calls
     * @see #getAggregation(String)
     * @see AggregationResult#getCardinality()
     */
    public ElasticQuery<E> addCardinalityAggregation(String name, Mapping field) {
        return addAggregation(AggregationBuilder.createCardinality(name, field));
    }

    /**
     * Adds a cardinality aggregation for the given field.
     *
     * @param name      the name of the aggregation
     * @param field     the field to aggregate
     * @param precision the precision threshold to apply. This contains the number up to which the returned cardinality
     *                  is exact.
     * @return the query itself for fluent method calls
     * @see #getAggregation(String)
     * @see AggregationResult#getCardinality()
     */
    public ElasticQuery<E> addCardinalityAggregation(String name, Mapping field, int precision) {
        return addAggregation(AggregationBuilder.createCardinality(name, field)
                                                .addBodyParameter(AggregationBuilder.PRECISION_THRESHOLD, precision));
    }

    /**
     * Adds an aggregation counting all matching entities for the given filters.
     *
     * @return the query itself for fluent method calls
     * @see #getAggregatedTotalHits()
     */
    public ElasticQuery<E> addTotalHitsAggregation() {
        return addAggregation(AggregationBuilder.createValueCount(KEY_TOTAL, ElasticEntity.ID));
    }

    /**
     * Adds a date (bucket) aggregation.
     *
     * @param name   the name of the aggregation
     * @param field  the field to aggregate
     * @param ranges the ranges / buckets to aggregate
     * @return the query itself for fluent method calls
     * @see #getAggregation(String)
     * @see AggregationResult#forEachBucket(Consumer)
     */
    public ElasticQuery<E> addDateAggregation(String name, Mapping field, List<DateRange> ranges) {
        List<ObjectNode> transformedRanges = ranges.stream().map(range -> {
            ObjectNode result = Json.createObject().put(KEY_KEY, range.getKey());
            if (range.getFrom() != null) {
                result.put(KEY_FROM, DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(range.getFrom()));
            }
            if (range.getUntil() != null) {
                result.put(KEY_TO, DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(range.getUntil()));
            }
            return result;
        }).toList();

        return addAggregation(AggregationBuilder.create(KEY_DATE_RANGE, name)
                                                .addBodyParameter(KEY_FIELD, field.toString())
                                                .addBodyParameter(KEY_KEYED, true)
                                                .addBodyParameter(KEY_RANGES, transformedRanges));
    }

    /**
     * Specifies the routing value to use.
     * <p>
     * For routed entities it is highly recommended supplying a routing value as it greatly improves the
     * search performance. If no routing value is available, use {@link #deliberatelyUnrouted()} to signal
     * the value was deliberately skipped. Otherwise, a warning will be emitted to support error tracing.
     *
     * @param value the value to use for routing
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> routing(String value) {
        this.routing = value;
        return this;
    }

    /**
     * Signals the routing is deliberately skipped as no routing value is available.
     *
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> deliberatelyUnrouted() {
        this.unrouted = true;
        return this;
    }

    /**
     * Builds the actual JSON query for <tt>_search</tt>
     *
     * @return the query as JSON
     */
    private ObjectNode buildPayload() {
        ObjectNode payload = Json.createObject();
        if (descriptor.isVersioned()) {
            payload.put(KEY_SEQ_NO_PRIMARY_TERM, true);
        }

        if (explain) {
            payload.put(KEY_EXPLAIN, true);
        }

        applyQuery(payload);

        if (nearestNeighborsSearch != null) {
            payload.set("knn", nearestNeighborsSearch.build());
        }

        if (sorts != null && !sorts.isEmpty()) {
            payload.putPOJO(KEY_SORT, sorts);

            if (searchAfter != null && !searchAfter.isEmpty()) {
                payload.putPOJO(KEY_SEARCH_AFTER, searchAfter);
            }
        }

        if (aggregations != null) {
            ObjectNode aggs = Json.createObject();
            aggregations.forEach(agg -> aggs.set(agg.getName(), agg.build()));
            payload.set(KEY_AGGS, aggs);
        }

        if (postFilters != null) {
            payload.set(KEY_POST_FILTER, postFilters.build());
        }

        if (Strings.isFilled(collapseBy)) {
            ObjectNode collapse = Json.createObject().put(KEY_FIELD, collapseBy);
            if (collapseByInnerHits != null) {
                collapse.putPOJO(KEY_INNER_HITS, collapseByInnerHits);
            }
            payload.set(KEY_COLLAPSE, collapse);
        }

        if (suggesters != null && !suggesters.isEmpty()) {
            payload.set(KEY_SUGGEST, Json.createObject().setAll(suggesters));
        }

        return payload;
    }

    /**
     * Creates a copy of the filters of this query.
     * <p>
     * This might be used e.g. in {@link sirius.db.es.suggest.SuggesterBuilder#collate(ObjectNode, boolean)}.
     *
     * @return a copy of the filters of this query
     */
    public ObjectNode getFilters() {
        if (queryBuilder == null) {
            return Json.createObject();
        }

        return queryBuilder.build();
    }

    /**
     * Adds the query and the function score query to the payload.
     * <p>
     * If a function score builder is set, it is used to wrap the query. Otherwise, the query is directly added to the
     * payload. If only a function score builder is set, it is added to the payload without a query.
     *
     * @param payload the existing payload to add the query to
     */
    private void applyQuery(ObjectNode payload) {
        if (functionScore != null) {
            if (queryBuilder != null) {
                payload.set(KEY_QUERY, functionScore.apply(queryBuilder.build()));
            } else {
                payload.set(KEY_QUERY, functionScore.build());
            }
        } else if (queryBuilder != null) {
            payload.set(KEY_QUERY, queryBuilder.build());
        }
    }

    /**
     * Builds a simplified JSON query for count and exists calls.
     *
     * @return the query as JSON
     */
    private ObjectNode buildSimplePayload() {
        ObjectNode payload = Json.createObject();

        if (queryBuilder != null) {
            payload.set(KEY_QUERY, queryBuilder.build());
        }

        return payload;
    }

    @Override
    public long count() {
        if (forceFail) {
            return 0;
        }

        String filteredRouting = checkRouting(Elastic.RoutingAccessMode.READ);

        ObjectNode countResponse = client.count(computeEffectiveIndexName(elastic::determineReadAlias),
                                                filteredRouting,
                                                buildSimplePayload());
        return countResponse.get(KEY_COUNT).asLong();
    }

    /**
     * Computes the effective index name to query.
     * <p>
     * In most cases this is simply the lowercase entity name (or its alias if one is present). However,
     * if additional descriptors are present, we concatenate all index names using "," e.g. <tt>index1,index2</tt>.
     *
     * @return the effective index name to use
     */
    private String computeEffectiveIndexName(Function<EntityDescriptor, String> aliasFunction) {
        if (additionalDescriptors == null) {
            return aliasFunction.apply(descriptor);
        } else {
            return Stream.concat(Stream.of(descriptor), additionalDescriptors.stream())
                         .map(aliasFunction)
                         .collect(Collectors.joining(","));
        }
    }

    private String filterRouting(Elastic.RoutingAccessMode accessMode) {
        if (Strings.isFilled(routing) && !elastic.isRoutingSuppressed(descriptor, accessMode)) {
            return routing;
        }

        return null;
    }

    private String checkRouting(Elastic.RoutingAccessMode accessMode) {
        String filteredRouting = filterRouting(accessMode);

        if (elastic.isRouted(descriptor, accessMode)) {
            if (Strings.isEmpty(filteredRouting) && !unrouted) {
                Elastic.LOG.WARN("""
                                         Trying query an entity of type '%s' without providing a routing! This will most probably return an invalid result!
                                         %s
                                         """, descriptor.getType().getName(), this, ExecutionPoint.snapshot());
            }
        } else if (Strings.isFilled(filteredRouting)) {
            Elastic.LOG.WARN("""
                                     Trying query an entity of type '%s' while providing a routing! This entity is unrouted! This will most probably return an invalid result!
                                     %s
                                     """, descriptor.getType().getName(), this, ExecutionPoint.snapshot());
        }

        return filteredRouting;
    }

    @Override
    public boolean exists() {
        if (forceFail) {
            return false;
        }

        String filteredRouting = checkRouting(Elastic.RoutingAccessMode.READ);

        ObjectNode existsResponse = client.exists(computeEffectiveIndexName(elastic::determineReadAlias),
                                                  filteredRouting,
                                                  buildSimplePayload());
        return existsResponse.at(Strings.apply("/%s/%s", KEY_HITS, KEY_TOTAL)).get(KEY_VALUE).asInt() >= 1;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void doIterate(Predicate<E> handler) {
        if (forceFail) {
            return;
        }
        if (accessBlockWise()) {
            streamBlockwise().takeWhile(handler).forEach(ignored -> {
            });
            return;
        }

        String filteredRouting = checkRouting(Elastic.RoutingAccessMode.READ);

        this.response = client.search(computeEffectiveIndexName(elastic::determineReadAlias),
                                      filteredRouting,
                                      skip,
                                      limit,
                                      buildPayload());
        for (Object obj : this.response.withArray(Strings.apply("/%s/%s", KEY_HITS, KEY_HITS))) {
            if (!handler.test((E) Elastic.make(descriptor, (ObjectNode) obj))) {
                return;
            }
        }
    }

    /**
     * Determines if this query should access the result block-wise.
     * <p>
     * The limit needs to be greater than <tt>{@link #MAX_LIST_SIZE} + 1</tt> for block-wise access because we query
     * <tt>{@link #MAX_LIST_SIZE} + 1</tt> elements when not explicitly setting a limit.
     *
     * @return <tt>true</tt> if this query should use block-wise access, <tt>false</tt> otherwise
     */
    private boolean accessBlockWise() {
        return limit == 0 || limit > MAX_LIST_SIZE + 1;
    }

    /**
     * Executes a request which just contains aggregations and will not fetch any search items in the request body.
     * <p>
     * The computed aggregations can be read via {@link #getAggregation(String)}.
     */
    public void computeAggregations() {
        if (forceFail) {
            throw new IllegalStateException("Aggregations can not be computed on a failed query.");
        }
        if (limit != 0) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .withSystemErrorMessage("When using 'computeAggregations' no search items are fetched,"
                                                    + " but the limit parameter was set != 0.")
                            .handle();
        }

        String filteredRouting = checkRouting(Elastic.RoutingAccessMode.READ);

        this.response = client.search(computeEffectiveIndexName(elastic::determineReadAlias),
                                      filteredRouting,
                                      skip,
                                      limit,
                                      buildPayload());
    }

    /**
     * Returns the aggregations as an {@link ObjectNode}.
     * <p>
     * Note that the query has to be executed before calling this method.
     *
     * @return the response as JSON
     */
    public ObjectNode getRawAggregations() {
        return Json.getObject(getRawResponse(), KEY_AGGREGATIONS);
    }

    /**
     * Determines if the query has been executed using either {@link #queryList()} or the like or
     * {@link #computeAggregations()}.
     *
     * @return <tt>true</tt> if the query has been executed, and a result is ready, <tt>false</tt> otherwise
     */
    public boolean isExecuted() {
        return response != null;
    }

    /**
     * Returns the aggregation result for the given aggregation.
     *
     * @param name the name of the aggregation to fetch the result for
     * @return the result of the given aggregation. This will never be <tt>null</tt> as empty values are handled
     * gracefully.
     */
    @Nonnull
    public AggregationResult getAggregation(String name) {
        ObjectNode object = getRawAggregations();
        for (String aggregationName : name.split("\\.")) {
            JsonNode child = object.get(aggregationName);
            if (!(child instanceof ObjectNode childObject)) {
                return AggregationResult.of(null);
            } else {
                object = childObject;
            }
        }

        return AggregationResult.of(object);
    }

    /**
     * Returns the response as an {@link ObjectNode}.
     * <p>
     * Note that the query has to be executed before calling this method.
     *
     * @return the response as JSON
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Explain("Performing a deep copy of the whole object is most probably an overkill here.")
    public ObjectNode getRawResponse() {
        if (response == null) {
            if (accessBlockWise()) {
                throw Exceptions.handle()
                                .to(Mixing.LOG)
                                .withSystemErrorMessage(
                                        "Error while reading entities of type '%s': 'getRawResponse' cannot be accessed when iterating block-wise!",
                                        descriptor.getType().getSimpleName())
                                .handle();
            } else {
                throw Exceptions.handle()
                                .to(Mixing.LOG)
                                .withSystemErrorMessage(
                                        "Error while reading entities of type '%s': Cannot access the response using 'getRawResponse' before a query is executed!",
                                        descriptor.getType().getSimpleName())
                                .handle();
            }
        }

        return response;
    }

    /**
     * Returns the total number of hits (with a maximum of 10000) for this query.
     * <p>
     * If more than 10000 hits are expected an appropriate aggregation should be {@link #addTotalHitsAggregation() added}
     * and {@link #getAggregatedTotalHits() evaluated} instead, to get the real count.
     *
     * @return the total number of this (even when only {@link #computeAggregations()} was used).
     */
    public long getTotalHits() {
        return getRawResponse().at(Strings.apply("/hits/%s/%s", KEY_TOTAL, KEY_VALUE)).asLong();
    }

    /**
     * Returns the number of total hits computed as an aggregation while executing the query.
     * <p>
     * Note that the query has to be executed before calling this method.
     *
     * @return the number of total hits in the field
     * @see #addTotalHitsAggregation()
     */
    public long getAggregatedTotalHits() {
        return getAggregation(KEY_TOTAL).getValueCount().orElse(0);
    }

    /**
     * Returns the shards which were involved in this query.
     *
     * @return the total number of shards which have been queried
     */
    public long getNumShards() {
        return getRawResponse().at("/_shards/" + KEY_TOTAL).asLong();
    }

    @Override
    public Stream<E> streamBlockwise() {
        if (forceFail) {
            return Stream.empty();
        }

        if (limit > 0) {
            throw new UnsupportedOperationException("ElasticQuery doesn't allow 'limit' in streamBlockwise");
        }
        if (skip > 0) {
            throw new UnsupportedOperationException("ElasticQuery doesn't allow 'skip' in streamBlockwise");
        }

        // Note we use this "hack" of a stream of streams + flatMap so that our spliterator is guaranteed to
        // be closed once the stream is terminated. Note that Stream actually implements AutoClosable - however,
        // almost no stream is wrapped in a proper try-with-resources - and even IntelliJ doesn't care if the
        // stream remains open. However, Stream.flatMap has this nice guarantee of closing any incoming stream
        // automatically...
        ElasticBlockWiseSpliterator spliterator = new ElasticBlockWiseSpliterator();
        return Stream.of(StreamSupport.stream(spliterator, false).onClose(spliterator::close))
                     .flatMap(Function.identity());
    }

    private class ElasticBlockWiseSpliterator extends PullBasedSpliterator<E> {
        private final TaskContext taskContext = TaskContext.get();
        private String pit = null;
        private List<String> searchAfter = null;

        @Override
        public int characteristics() {
            return NONNULL | IMMUTABLE;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected Iterator<E> pullNextBlock() {
            if (!taskContext.isActive()) {
                return null;
            }

            String alias = computeEffectiveIndexName(elastic::determineReadAlias);
            String filterRouting = checkRouting(Elastic.RoutingAccessMode.READ);
            if (pit == null) {
                // first call to this method
                orderAsc(ElasticEntity.ID);
                pit = client.createPit(alias, filterRouting, STREAM_BLOCKWISE_PIT_TTL);
            } else {
                searchAfter(searchAfter);
            }

            ObjectNode payload = buildPayload().putPOJO(KEY_PIT,
                                                        Map.of(KEY_PIT_ID,
                                                               pit,
                                                               KEY_PIT_KEEP_ALIVE,
                                                               STREAM_BLOCKWISE_PIT_TTL));
            int maxResults = filterRouting != null ? BLOCK_SIZE_FOR_SINGLE_SHARD : BLOCK_SIZE_PER_SHARD;
            response = client.search("", null, 0, maxResults, payload);
            searchAfter = getLastSortValues();

            return Json.streamEntries(response.withArray(Strings.apply("/%s/%s", KEY_HITS, KEY_HITS)))
                       .map(obj -> (E) Elastic.make(descriptor, (ObjectNode) obj))
                       .iterator();
        }

        private void close() {
            if (pit != null) {
                client.closePit(pit);
            }
        }
    }

    @Override
    public void delete(@Nullable Consumer<E> entityCallback) {
        iterateAll(entity -> {
            if (entityCallback != null) {
                entityCallback.accept(entity);
            }

            elastic.delete(entity);
        });
    }

    @Override
    public void truncate() {
        elastic.retry(this::tryTruncate);
    }

    /**
     * Deletes all matches using the capabilities of the underlying database.
     * <p>
     * Therefore <b>no checks</b> or anything will be invoked for the deleted entities.
     * <p>
     * Use this for larger result sets where integrity and constraints do not matter or are managed manually.
     *
     * @throws OptimisticLockException               if one of the documents was modified during the runtime of the truncate
     * @throws sirius.kernel.health.HandledException if the {@linkplain LowLevelClient#deleteByQuery(String, String, ObjectNode) deleteByQuery}
     *                                               request aborted due to any unrecoverable errors during the process
     */
    public void tryTruncate() throws OptimisticLockException {
        if (forceFail) {
            return;
        }

        String filteredRouting = checkRouting(Elastic.RoutingAccessMode.WRITE);
        ObjectNode deleteByQueryResponse = elastic.getLowLevelClient()
                                                  .deleteByQuery(computeEffectiveIndexName(elastic::determineWriteAlias),
                                                                 filteredRouting,
                                                                 buildSimplePayload());
        if (Boolean.TRUE.equals(deleteByQueryResponse.get(KEY_TIMED_OUT).asBoolean())
            || deleteByQueryResponse.get(KEY_VERSION_CONFLICTS).asInt() > 0) {
            Elastic.LOG.WARN("Truncate timed out or had version conflicts:\n" + Json.write(deleteByQueryResponse));
            throw new OptimisticLockException();
        }
        ArrayNode failures = deleteByQueryResponse.withArray(JsonPointer.SEPARATOR + KEY_FAILURES);
        if (failures != null && !failures.isEmpty()) {
            Elastic.LOG.SEVERE("Truncate aborted due to unrecoverable error(s):\n" + Json.write(deleteByQueryResponse));
            throw Exceptions.createHandled()
                            .withSystemErrorMessage("Truncate aborted due to unrecoverable error(s)!")
                            .handle();
        }
    }

    @Override
    public FilterFactory<ElasticConstraint> filters() {
        return Elastic.FILTERS;
    }

    @Override
    public String toString() {
        return descriptor.getType() + ": " + buildPayload();
    }
}
