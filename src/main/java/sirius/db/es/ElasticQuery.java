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
import sirius.db.es.constraints.BoolQueryBuilder;
import sirius.db.es.constraints.ElasticConstraint;
import sirius.db.es.suggest.SuggestBuilder;
import sirius.db.es.suggest.SuggestOption;
import sirius.db.es.suggest.SuggestPart;
import sirius.db.mixing.DateRange;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.query.Query;
import sirius.db.mixing.query.constraints.FilterFactory;
import sirius.kernel.async.ExecutionPoint;
import sirius.kernel.async.TaskContext;
import sirius.kernel.commons.Limit;
import sirius.kernel.commons.RateLimit;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;

import javax.annotation.Nullable;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Provides a fluent query API for Elasticsearch.
 *
 * @param <E> the type of entities to be queried
 */
public class ElasticQuery<E extends ElasticEntity> extends Query<ElasticQuery<E>, E, ElasticConstraint> {

    /**
     * Contains the default number of buckets being collected and reported for an aggregation.
     */
    public static final int DEFAULT_TERM_AGGREGATION_BUCKET_COUNT = 25;

    private static final int SCROLL_TTL_SECONDS = 60 * 5;
    private static final int MAX_SCROLL_RESULTS_FOR_SINGLE_SHARD = 50;
    private static final int MAX_SCROLL_RESULTS_PER_SHARD = 10;
    private static final String KEY_SCROLL_ID = "_scroll_id";
    private static final String KEY_DOC_ID = "_doc";

    private static final String KEY_FIELD = "field";
    private static final String KEY_TERMS = "terms";
    private static final String KEY_SIZE = "size";
    private static final String KEY_DATE_RANGE = "date_range";
    private static final String KEY_KEYED = "keyed";
    private static final String KEY_RANGES = "ranges";
    private static final String KEY_QUERY = "query";
    private static final String KEY_SORT = "sort";
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
    private static final String KEY_SEQ_NO_PRIMARY_TERM = "seq_no_primary_term";
    private static final Mapping SCORE = Mapping.named("_score");

    @Part
    private static Elastic elastic;

    @Part
    private static IndexMappings indexMappings;

    private final LowLevelClient client;

    private BoolQueryBuilder queryBuilder;

    private List<AggregationBuilder> aggregations;

    private BoolQueryBuilder postFilters;

    private String collapseBy;
    private List<JSONObject> collapseByInnerHits;

    private List<JSONObject> sorts;

    private FunctionScoreBuilder functionScore;

    private String routing;
    private boolean unrouted;

    private boolean explain;

    private Map<String, JSONObject> suggesters;

    private JSONObject response;

    /**
     * Used to describe inner hits which are determine for field collapsing.
     * <p>
     * Note that there is no <tt>build()</tt> method, as the constructor already applies the object to the query.
     */
    public class InnerHitsBuilder {
        private final JSONObject json;
        private List<JSONObject> sorts;

        protected InnerHitsBuilder(String name, int size) {
            this.json = new JSONObject().fluentPut(KEY_NAME, name).fluentPut(KEY_SIZE, size);
            ElasticQuery.this.collapseByInnerHits = autoinit(ElasticQuery.this.collapseByInnerHits);
            ElasticQuery.this.collapseByInnerHits.add(json);
        }

        /**
         * Adds a sort criteria for the given field, sorting ascending
         *
         * @param field the field to sort by
         * @return the builder itself for fluent method calls
         */
        public InnerHitsBuilder orderByAsc(String field) {
            if (this.sorts == null) {
                this.sorts = new ArrayList<>();
                this.json.put(KEY_SORT, sorts);
            }
            this.sorts.add(new JSONObject().fluentPut(field, KEY_ASC));
            return this;
        }

        /**
         * Adds a sort criteria for the given field, sorting descending
         *
         * @param field the field to sort by
         * @return the builder itself for fluent method calls
         */
        public InnerHitsBuilder orderByDesc(String field) {
            if (this.sorts == null) {
                this.sorts = new ArrayList<>();
                this.json.put(KEY_SORT, sorts);
            }
            this.sorts.add(new JSONObject().fluentPut(field, KEY_DESC));
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
            this.json.put(name, value);
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
     * note that we perform a shallow copy. Therefore if a query is e.g. supplier with an inner hits
     * builder (via {@link #addCollapsedInnerHits(String, int)} and then copied, the builder will
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

        if (functionScore != null) {
            copy.functionScore = this.functionScore.copy();
        }

        if (suggesters != null) {
            copy.suggesters = this.suggesters.entrySet()
                                             .stream()
                                             .collect(Collectors.toMap(Map.Entry::getKey,
                                                                       entry -> (JSONObject) entry.getValue().clone()));
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
     * Enables the explain mode which gives detailed informations about score calculations.
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
     * Adds a MUST filter to the query.
     *
     * @param filter the filter to add
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> must(JSONObject filter) {
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
    public ElasticQuery<E> mustNot(JSONObject filter) {
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
     * @see BoolQueryBuilder#filter(JSONObject)
     */
    public ElasticQuery<E> filter(JSONObject filter) {
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
     * Adds a post filterto the query.
     *
     * @param filter the filter to add
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> postFilter(JSONObject filter) {
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
    public ElasticQuery<E> sort(JSONObject sortSpec) {
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
    public ElasticQuery<E> sort(Mapping field, JSONObject sortSpec) {
        return sort(new JSONObject().fluentPut(field.toString(), sortSpec));
    }

    /**
     * Adds an ascending sort by the given field to the query.
     *
     * @param field the field to order by
     * @return the query itself for fluent method calls
     */
    @Override
    public ElasticQuery<E> orderAsc(Mapping field) {
        return sort(field, new JSONObject().fluentPut(KEY_ORDER, KEY_ASC));
    }

    /**
     * Adds a descending sort by the given field to the query.
     *
     * @param field the field to order by
     * @return the query itself for fluent method calls
     */
    @Override
    public ElasticQuery<E> orderDesc(Mapping field) {
        return sort(field, new JSONObject().fluentPut(KEY_ORDER, KEY_DESC));
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
     * Collapses by the given field.
     *
     * @param field the field to collapse results by.
     * @return the query itself for fluent method calls
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
     * Adds a term (bucket) aggregation for the given field.
     *
     * @param field the field to aggregate
     * @return the query itself for fluent method calls
     * @see #getAggregationBuckets(String)
     */
    public ElasticQuery<E> addTermAggregation(Mapping field) {
        return addTermAggregation(field.toString(), field, DEFAULT_TERM_AGGREGATION_BUCKET_COUNT);
    }

    /**
     * Adds a term (bucket) aggregation for the given field.
     *
     * @param name  the name of the aggregation
     * @param field the field to aggregate
     * @param size  the max. number of buckets to return
     * @return the query itself for fluent method calls
     * @see #getAggregationBuckets(String)
     */
    public ElasticQuery<E> addTermAggregation(String name, Mapping field, int size) {
        return addAggregation(AggregationBuilder.create(KEY_TERMS, name)
                                                .addBodyParameter(KEY_FIELD, field.toString())
                                                .addBodyParameter(KEY_SIZE, size));
    }

    /**
     * Adds a date (bucket) aggregation.
     *
     * @param name   the name of the aggregation
     * @param field  the field to aggregate
     * @param ranges the ranges / buckets to aggregate
     * @return the query itself for fluent method calls
     * @see #getAggregationBuckets(String)
     */
    public ElasticQuery<E> addDateAggregation(String name, Mapping field, List<DateRange> ranges) {
        List<JSONObject> transformedRanges = ranges.stream().map(range -> {
            JSONObject result = new JSONObject().fluentPut(KEY_KEY, range.getKey());
            if (range.getFrom() != null) {
                result.fluentPut(KEY_FROM, DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(range.getFrom()));
            }
            if (range.getUntil() != null) {
                result.fluentPut(KEY_TO, DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(range.getUntil()));
            }
            return result;
        }).collect(Collectors.toList());

        return addAggregation(AggregationBuilder.create(KEY_DATE_RANGE, name)
                                                .addBodyParameter(KEY_FIELD, field.toString())
                                                .addBodyParameter(KEY_KEYED, true)
                                                .addBodyParameter(KEY_RANGES, transformedRanges));
    }

    /**
     * Adds a suggester.
     *
     * @param name    the name of the suggester
     * @param suggest a JSON object describing a suggest requirement
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> suggest(String name, JSONObject suggest) {
        if (this.suggesters == null) {
            this.suggesters = new HashMap<>();
        }
        suggesters.put(name, suggest);
        return this;
    }

    /**
     * Adds a suggester.
     *
     * @param suggestBuilder a suggest builder
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> suggest(SuggestBuilder suggestBuilder) {
        return suggest(suggestBuilder.getName(), suggestBuilder.build());
    }

    /**
     * Specifies the routing value to use.
     * <p>
     * For routed entities it is highly recommended to supply a routing value as it greatly improves the
     * search performance. If no routing value is available, use {@link #deliberatelyUnrouted()} to signal
     * the the value was deliberately skipped. Otherwise a warning will be emitted to support error tracing.
     *
     * @param value the value to use for routing
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> routing(String value) {
        this.routing = value;
        return this;
    }

    /**
     * Signals the the routing is deliberately skipped as no routing value is available.
     *
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> deliberatelyUnrouted() {
        this.unrouted = true;
        return this;
    }

    /**
     * Builds the acutal JSON query for <tt>_search</tt>
     *
     * @return the query as JSON
     */
    private JSONObject buildPayload() {
        JSONObject payload = new JSONObject();
        if (descriptor.isVersioned()) {
            payload.put(KEY_SEQ_NO_PRIMARY_TERM, true);
        }

        if (explain) {
            payload.put(KEY_EXPLAIN, true);
        }

        applyQuery(payload);

        if (sorts != null && !sorts.isEmpty()) {
            payload.put(KEY_SORT, sorts);
        }

        if (aggregations != null) {
            JSONObject aggs = new JSONObject();
            aggregations.forEach(agg -> aggs.put(agg.getName(), agg.build()));
            payload.put(KEY_AGGS, aggs);
        }

        if (postFilters != null) {
            payload.put(KEY_POST_FILTER, postFilters.build());
        }

        if (Strings.isFilled(collapseBy)) {
            JSONObject collapse = new JSONObject().fluentPut(KEY_FIELD, collapseBy);
            if (collapseByInnerHits != null) {
                collapse.put(KEY_INNER_HITS, collapseByInnerHits);
            }
            payload.put(KEY_COLLAPSE, collapse);
        }

        if (suggesters != null && !suggesters.isEmpty()) {
            payload.put(KEY_SUGGEST, new JSONObject().fluentPutAll(suggesters));
        }

        return payload;
    }

    /**
     * Adds the query and the function score query to the payload.
     * <p>
     * If a function score builder is set, it is used to wrap the query. Otherwise the query is directly added to the
     * payload. If only a function score builder is set, it is added to the payload without a query.
     *
     * @param payload the existing payload to add the query to
     */
    private void applyQuery(JSONObject payload) {
        if (functionScore != null) {
            if (queryBuilder != null) {
                payload.put(KEY_QUERY, functionScore.apply(queryBuilder.build()));
            } else {
                payload.put(KEY_QUERY, functionScore.build());
            }
        } else if (queryBuilder != null) {
            payload.put(KEY_QUERY, queryBuilder.build());
        }
    }

    /**
     * Builds a simplified JSON query for count and exists calls.
     *
     * @return the query as JSON
     */
    private JSONObject buildSimplePayload() {
        JSONObject payload = new JSONObject();

        if (queryBuilder != null) {
            payload.put(KEY_QUERY, queryBuilder.build());
        }

        return payload;
    }

    @Override
    public long count() {
        if (skip > 0 || limit > 0) {
            Elastic.LOG.WARN("COUNT queries support neither skip nor limit: %s\n%s", this, ExecutionPoint.snapshot());
        }

        String filteredRouting = checkRouting(Elastic.RoutingAccessMode.READ);

        JSONObject countResponse =
                client.count(elastic.determineReadAlias(descriptor), filteredRouting, buildSimplePayload());
        return countResponse.getLong(KEY_COUNT);
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
                Elastic.LOG.WARN("Trying query an entity of type '%s' without providing a routing!"
                                 + " This will most probably return an invalid result!\n%s\n",
                                 descriptor.getType().getName(),
                                 this,
                                 ExecutionPoint.snapshot());
            }
        } else if (Strings.isFilled(filteredRouting)) {
            Elastic.LOG.WARN("Trying query an entity of type '%s' while providing a routing! This entity is unrouted!"
                             + " This will most probably return an invalid result!\n%s\n",
                             descriptor.getType().getName(),
                             this,
                             ExecutionPoint.snapshot());
        }

        return filteredRouting;
    }

    @Override
    public boolean exists() {
        if (skip > 0 || limit > 0) {
            Elastic.LOG.WARN("EXISTS queries support neither skip nor limit: %s\n%s", this, ExecutionPoint.snapshot());
        }

        String filteredRouting = checkRouting(Elastic.RoutingAccessMode.READ);

        JSONObject existsResponse =
                client.exists(elastic.determineReadAlias(descriptor), filteredRouting, buildSimplePayload());
        return existsResponse.getJSONObject(KEY_HITS).getJSONObject(KEY_TOTAL).getIntValue("value") >= 1;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void iterate(Predicate<E> handler) {
        if (useScrolling()) {
            scroll(handler);
            return;
        }

        String filteredRouting = checkRouting(Elastic.RoutingAccessMode.READ);

        this.response =
                client.search(elastic.determineReadAlias(descriptor), filteredRouting, skip, limit, buildPayload());
        for (Object obj : this.response.getJSONObject(KEY_HITS).getJSONArray(KEY_HITS)) {
            if (!handler.test((E) Elastic.make(descriptor, (JSONObject) obj))) {
                return;
            }
        }
    }

    /**
     * Determines if this query should use scrolling.
     * <p>
     * The limit needs to be greater than <tt>{@link #MAX_LIST_SIZE} + 1</tt> for scrolling because we query
     * <tt>{@link #MAX_LIST_SIZE} + 1</tt> elements when not explicitly setting a limit.
     *
     * @return <tt>true</tt> if this query should scroll
     */
    private boolean useScrolling() {
        return limit == 0 || limit > MAX_LIST_SIZE + 1;
    }

    /**
     * Executes a request which just contains aggregations and will not fetch any search items in the request body.
     * <p>
     * The computed aggregations can be read via {@link #getAggregationBuckets(String)}.
     */
    public void computeAggregations() {
        if (limit != 0) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .withSystemErrorMessage("When using 'computeAggregations' no search items are fetched,"
                                                    + " but the limit parameter was set != 0.")
                            .handle();
        }

        String filteredRouting = checkRouting(Elastic.RoutingAccessMode.READ);

        this.response =
                client.search(elastic.determineReadAlias(descriptor), filteredRouting, skip, limit, buildPayload());
    }

    /**
     * Returns the buckets which were computed as an aggregation while executing the query.
     * <p>
     * Note that the query has to be executed before calling this method.
     *
     * @param name the aggregation to read
     * @return the buckets which were computed for the given aggregation
     */
    public List<Tuple<String, Integer>> getAggregationBuckets(String name) {
        if (response == null && useScrolling()) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .withSystemErrorMessage("'getAggregationBuckets' not possible when scrolling")
                            .handle();
        }

        List<Tuple<String, Integer>> result = new ArrayList<>();

        JSONObject responseAggregations = response.getJSONObject(KEY_AGGREGATIONS);
        if (responseAggregations == null) {
            return result;
        }

        JSONObject aggregation = responseAggregations.getJSONObject(name);
        if (aggregation == null) {
            return result;
        }

        return Bucket.fromAggregation(aggregation)
                     .stream()
                     .map(bucket -> Tuple.create(bucket.getKey(), bucket.getDocCount()))
                     .collect(Collectors.toList());
    }

    /**
     * Returns the aggregations as a {@link JSONObject}.
     * <p>
     * Note that the query has to be executed before calling this method.
     *
     * @return the response as JSON
     */
    public JSONObject getRawAggregations() {
        if (response == null && useScrolling()) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .withSystemErrorMessage("'getRawAggregations' not possible when scrolling")
                            .handle();
        }

        return response.getJSONObject(KEY_AGGREGATIONS);
    }

    /**
     * Returns the response as a {@link JSONObject}.
     * <p>
     * Note that the query has to be executed before calling this method.
     *
     * @return the response as JSON
     */
    public JSONObject getRawResponse() {
        if (response == null && useScrolling()) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .withSystemErrorMessage("'getRawResponse' not possible when scrolling")
                            .handle();
        }

        return (JSONObject) response.clone();
    }

    /**
     * Returns the hits as a map of {@link JSONObject}s.
     * <p>
     * Note that the query has to be executed before calling this method.
     *
     * @return a map of the hits as JSON with the document ID as the key
     * @deprecated use {@link ElasticEntity#getSearchHit()}
     */
    @Deprecated
    public Map<String, JSONObject> getRawHits() {
        if (response == null && useScrolling()) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .withSystemErrorMessage("'getRawHits' not possible when scrolling")
                            .handle();
        }

        return response.getJSONObject(KEY_HITS)
                       .getJSONArray(KEY_HITS)
                       .stream()
                       .filter(hit -> hit instanceof JSONObject)
                       .map(hit -> (JSONObject) hit)
                       .collect(Collectors.toMap(hit -> hit.getString(Elastic.ID_FIELD), Function.identity()));
    }

    /**
     * Returns all suggest options for the suggester with the given name.
     *
     * @param name the name of the suggester
     * @return a list of suggest options
     */
    public List<SuggestOption> getSuggestOptions(String name) {
        return getSuggestParts(name).stream().flatMap(part -> part.getOptions().stream()).collect(Collectors.toList());
    }

    /**
     * Returns all suggest parts for the suggester with the given name.
     * <p>
     * This is mainly used for term suggesters where every term receives their own suggestions.
     *
     * @param name the name of the suggester
     * @return a list of suggest parts
     */
    public List<SuggestPart> getSuggestParts(String name) {
        if (response == null) {
            String filteredRouting = checkRouting(Elastic.RoutingAccessMode.READ);

            this.response =
                    client.search(elastic.determineReadAlias(descriptor), filteredRouting, skip, limit, buildPayload());
        }

        JSONObject responseSuggestions = response.getJSONObject(KEY_SUGGEST);

        if (responseSuggestions == null) {
            return Collections.emptyList();
        }

        return responseSuggestions.getJSONArray(name)
                                  .stream()
                                  .map(part -> (JSONObject) part)
                                  .map(SuggestPart::makeSuggestPart)
                                  .collect(Collectors.toList());
    }

    /**
     * For larger queries, we use a scroll query in Elasticsearch, which provides kind of a
     * cursor to fetch the results blockwise.
     *
     * @param handler the result handler as passed to {@link #iterate(Predicate)}
     */
    private void scroll(Predicate<E> handler) {
        try {
            if (sorts == null || sorts.isEmpty()) {
                // If no explicit search order is given, we sort by _doc which improves the performance
                // according to the Elasticsearch documentation.
                orderAsc(Mapping.named(KEY_DOC_ID));
            }

            String filteredRouting = checkRouting(Elastic.RoutingAccessMode.READ);

            JSONObject scrollResponse = client.createScroll(elastic.determineReadAlias(descriptor),
                                                            filteredRouting,
                                                            0,
                                                            filteredRouting == null ?
                                                            MAX_SCROLL_RESULTS_FOR_SINGLE_SHARD :
                                                            MAX_SCROLL_RESULTS_PER_SHARD,
                                                            SCROLL_TTL_SECONDS,
                                                            buildPayload());
            try {
                TaskContext ctx = TaskContext.get();
                RateLimit rateLimit = RateLimit.timeInterval(1, TimeUnit.SECONDS);
                Limit effectiveLimit = new Limit(skip, limit);
                scrollResponse = executeScroll(entity -> {
                    // Check if the user aborted processing...
                    if (rateLimit.check() && !ctx.isActive()) {
                        return false;
                    }

                    // If we are still skipping items, quickly process the next one...
                    if (!effectiveLimit.nextRow()) {
                        return true;
                    }

                    // Process entity, abort if the handler isn't interested in continuing...
                    if (!handler.test(entity)) {
                        return false;
                    }

                    // Let the limit deciede if we should continue or not...
                    return effectiveLimit.shouldContinue();
                }, scrollResponse);
            } finally {
                client.closeScroll(scrollResponse.getString(KEY_SCROLL_ID));
            }
        } catch (Exception t) {
            throw Exceptions.handle(Elastic.LOG, t);
        }
    }

    /**
     * Loops over the scroll cursor until either processing is aborted or all entities have been read.
     *
     * @param handler       the handler which processes the entity and determines if we should continue
     * @param firstResponse the first response we received when creating the scroll query.
     * @return the last response we received when iterating over the scroll query
     */
    @SuppressWarnings("unchecked")
    private JSONObject executeScroll(Predicate<E> handler, JSONObject firstResponse) {
        long lastScroll = 0;
        JSONObject scrollResponse = firstResponse;
        while (true) {
            // we keep on executing queries until es returns an empty list of results...
            JSONArray hits = scrollResponse.getJSONObject(KEY_HITS).getJSONArray(KEY_HITS);
            if (hits.isEmpty()) {
                return scrollResponse;
            }

            for (Object obj : hits) {
                if (!handler.test((E) Elastic.make(descriptor, (JSONObject) obj))) {
                    return scrollResponse;
                }
            }

            lastScroll = performScrollMonitoring(lastScroll);
            scrollResponse = client.continueScroll(SCROLL_TTL_SECONDS, scrollResponse.getString(KEY_SCROLL_ID));
        }
    }

    /**
     * As a scroll cursor can timeout, we monitor the call interval and emit a warning if a timeout might have occurred.
     *
     * @param lastScroll the timestamp when the last scoll was executed
     * @return the next timestamp
     */
    private long performScrollMonitoring(long lastScroll) {
        long now = System.currentTimeMillis();
        if (lastScroll > 0) {
            long deltaInSeconds = TimeUnit.SECONDS.convert(now - lastScroll, TimeUnit.MILLISECONDS);
            // Warn if processing of one scroll took longer thant our keep alive....
            if (deltaInSeconds > SCROLL_TTL_SECONDS) {
                Exceptions.handle()
                          .withSystemErrorMessage(
                                  "A scroll query against elasticserach took too long to process its data! "
                                  + "The result is probably inconsistent! Query: %s\n%s",
                                  this,
                                  ExecutionPoint.snapshot())
                          .to(Elastic.LOG)
                          .handle();
            }
        }
        return now;
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
        String filteredRouting = checkRouting(Elastic.RoutingAccessMode.WRITE);
        elastic.getLowLevelClient()
               .deleteByQuery(elastic.determineWriteAlias(descriptor), filteredRouting, buildSimplePayload());
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
