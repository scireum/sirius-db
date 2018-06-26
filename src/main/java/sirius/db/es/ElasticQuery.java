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
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.DateRange;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
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

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Provides a fluent query API for Elasticsearch.
 *
 * @param <E> the type of entities to be queried
 */
public class ElasticQuery<E extends ElasticEntity> extends Query<ElasticQuery<E>, E, ElasticConstraint> {

    private static final int SCROLL_TTL_SECONDS = 60 * 5;
    private static final int MAX_SCROLL_RESULTS_FOR_SINGLE_SHARD = 50;
    private static final int MAX_SCROLL_RESULTS_PER_SHARD = 10;
    private static final String RESPONSE_SCROLL_ID = "_scroll_id";
    private static final String KEY_DOC_ID = "_doc";
    private static final int DEFAULT_TERM_AGGREGATION_BUCKET_COUNT = 25;

    @Part
    private static Elastic elastic;

    @Part
    private static IndexMappings indexMappings;

    private final LowLevelClient client;

    private BoolQueryBuilder queryBuilder;

    private JSONObject aggregations;

    private List<JSONObject> postFilters;

    private String collapseBy;
    private List<JSONObject> collapseByInnerHits;

    private List<JSONObject> sorts;

    private List<Integer> years;

    private String routing;
    private boolean unrouted;

    private JSONObject response;

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

    private <X> List<X> autoinit(List<X> list) {
        if (list == null) {
            return new ArrayList<>();
        }

        return list;
    }

    /**
     * Specifies which years to query for entities which are {@link sirius.db.es.annotations.StorePerYear stored per year}.
     *
     * @param years a list of years to search in. Note that years for which no index exists will be filtered automatically
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> years(int... years) {
        this.years = autoinit(this.years);
        Arrays.stream(years).forEach(this.years::add);
        return this;
    }

    /**
     * Specifies a range of years to query for entities which are {@link sirius.db.es.annotations.StorePerYear stored per year}.
     * <p>
     * Note that years for which no index exists will be filtered automatically
     *
     * @param from the first year to search in
     * @param to   the last year to search in
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> yearsFromTo(int from, int to) {
        this.years = autoinit(this.years);
        for (int year = from; year <= to; year++) {
            years.add(year);
        }
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
        return sort(field, new JSONObject().fluentPut("order", "asc"));
    }

    /**
     * Adds a descending sort by the given field to the query.
     *
     * @param field the field to order by
     * @return the query itself for fluent method calls
     */
    @Override
    public ElasticQuery<E> orderDesc(Mapping field) {
        return sort(field, new JSONObject().fluentPut("order", "desc"));
    }

    // TODO
    public ElasticQuery<E> collapse(Mapping field) {
        this.collapseBy = field.toString();
        return this;
    }

    // TODO
    public ElasticQuery<E> collapse(String field) {
        this.collapseBy = field;
        return this;
    }

    /**
     * Adds the given aggregation for the given name.
     *
     * @param name        the name of the aggregation
     * @param aggregation the aggregation itself
     * @return the query itself for fluent method calls
     */
    public ElasticQuery<E> addAggregation(String name, JSONObject aggregation) {
        if (aggregations == null) {
            aggregations = new JSONObject();
        }
        aggregations.put(name, aggregation);

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
        return addAggregation(name,
                              new JSONObject().fluentPut("terms",
                                                         new JSONObject().fluentPut("field", field.toString())
                                                                         .fluentPut("size", size)));
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
            JSONObject result = new JSONObject().fluentPut("key", range.getKey());
            if (range.getFrom() != null) {
                result.fluentPut("from", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(range.getFrom()));
            }
            if (range.getUntil() != null) {
                result.fluentPut("to", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(range.getUntil()));
            }
            return result;
        }).collect(Collectors.toList());

        return addAggregation(name,
                              new JSONObject().fluentPut("date_range",
                                                         new JSONObject().fluentPut("field", field.toString())
                                                                         .fluentPut("keyed", true)
                                                                         .fluentPut("ranges", transformedRanges)));
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
            payload.put(BaseMapper.VERSION, true);
        }

        if (queryBuilder != null) {
            payload.put("query", queryBuilder.build());
        }

        if (sorts != null && !sorts.isEmpty()) {
            payload.put("sort", sorts);
        }

        if (aggregations != null) {
            payload.put("aggs", aggregations);
        }

        return payload;
    }

    /**
     * Builds a simplified JSON query for count and exists calls.
     *
     * @return the query as JSON
     */
    private JSONObject buildSimplePayload() {
        JSONObject payload = new JSONObject();

        if (queryBuilder != null) {
            payload.put("query", queryBuilder.build());
        }

        return payload;
    }

    @Override
    public long count() {
        if (skip > 0 || limit > 0) {
            Elastic.LOG.WARN("COUNT queries support neither skip nor limit: %s\n%s", this, ExecutionPoint.snapshot());
        }

        checkRouting();

        List<String> indices = determineIndices();

        if (indices.isEmpty()) {
            return 0;
        }

        JSONObject countResponse =
                client.count(indices, elastic.determineTypeName(descriptor), routing, buildSimplePayload());
        return countResponse.getLong("count");
    }

    private void checkRouting() {
        if (elastic.isRouted(descriptor)) {
            if (Strings.isEmpty(routing) && !unrouted) {
                Elastic.LOG.WARN("Trying query an entity of type '%s' without providing a routing!"
                                 + " This will most probably return an invalid result!\n%s\n",
                                 descriptor.getType().getName(),
                                 this,
                                 ExecutionPoint.snapshot());
            }
        } else if (Strings.isFilled(routing)) {
            Elastic.LOG.WARN("Trying query an entity of type '%s' while providing a routing! This entity is unrouted!"
                             + " This will most probably return an invalid result!\n%s\n",
                             descriptor.getType().getName(),
                             this,
                             ExecutionPoint.snapshot());
        }
    }

    /**
     * Determines which indices top search in.
     *
     * @return the list of indices to search in
     */
    private List<String> determineIndices() {
        if (!elastic.isStoredPerYear(descriptor)) {
            if (years != null && !years.isEmpty()) {
                Elastic.LOG.WARN(
                        "Discriminators (years) were given for an entity (%s) which isn't stored per year: %s\n",
                        descriptor.getType(),
                        this,
                        ExecutionPoint.snapshot());
            }

            return Collections.singletonList(elastic.determineIndex(descriptor, null));
        } else {
            if (years == null || years.isEmpty()) {
                Elastic.LOG.WARN(
                        "No discriminators (years) were given for an entity (%s) which isn't stored per year: %s\n",
                        descriptor.getType(),
                        this,
                        ExecutionPoint.snapshot());
            }

            if (years == null) {
                return Collections.emptyList();
            }

            return years.stream()
                        .map(year -> elastic.determineYearIndex(descriptor, String.valueOf(year)))
                        .filter(indexMappings::yearlyIndexExists)
                        .collect(Collectors.toList());
        }
    }

    @Override
    public boolean exists() {
        if (skip > 0 || limit > 0) {
            Elastic.LOG.WARN("EXISTS queries support neither skip nor limit: %s\n%s", this, ExecutionPoint.snapshot());
        }

        checkRouting();

        List<String> indices = determineIndices();

        if (indices.isEmpty()) {
            return false;
        }

        JSONObject existsResponse =
                client.exists(indices, elastic.determineTypeName(descriptor), routing, buildSimplePayload());
        return existsResponse.getJSONObject("hits").getInteger("total") >= 1;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void iterate(Function<E, Boolean> handler) {
        if (limit == 0 || limit > MAX_LIST_SIZE) {
            scroll(handler);
            return;
        }

        checkRouting();

        List<String> indices = determineIndices();
        if (indices.isEmpty()) {
            return;
        }

        this.response =
                client.search(indices, elastic.determineTypeName(descriptor), routing, skip, limit, buildPayload());
        for (Object obj : this.response.getJSONObject("hits").getJSONArray("hits")) {
            if (!handler.apply((E) Elastic.make(descriptor, (JSONObject) obj))) {
                return;
            }
        }
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
        List<Tuple<String, Integer>> result = new ArrayList<>();

        JSONObject responseAggregations = response.getJSONObject("aggregations");
        if (responseAggregations == null) {
            return result;
        }

        JSONObject aggregation = responseAggregations.getJSONObject(name);
        if (aggregation == null) {
            return result;
        }

        Object buckets = aggregation.get("buckets");
        if (buckets instanceof JSONArray) {
            for (Object bucket : (JSONArray) buckets) {
                result.add(Tuple.create(((JSONObject) bucket).getString("key"),
                                        ((JSONObject) bucket).getInteger("doc_count")));
            }
        } else if (buckets instanceof JSONObject) {
            for (Map.Entry<String, Object> entry : ((JSONObject) buckets).entrySet()) {
                result.add(Tuple.create(entry.getKey(), ((JSONObject) entry.getValue()).getInteger("doc_count")));
            }
        }

        return result;
    }

    /**
     * For larger queries, we use a scroll query in Elasticsearch, which provides kind of a
     * cursor to fetch the results blockwise.
     *
     * @param handler the result handler as passed to {@link #iterate(Function)}
     */
    private void scroll(Function<E, Boolean> handler) {
        try {
            if (sorts == null || sorts.isEmpty()) {
                // If no explicit search order is given, we sort by _doc which improves the performance
                // according to the Elasticsearch documentation.
                orderAsc(Mapping.named(KEY_DOC_ID));
            }

            checkRouting();

            List<String> indices = determineIndices();
            if (indices.isEmpty()) {
                return;
            }

            JSONObject scrollResponse = client.createScroll(indices,
                                                            elastic.determineTypeName(descriptor),
                                                            routing,
                                                            0,
                                                            routing == null ?
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
                    if (!handler.apply(entity)) {
                        return false;
                    }

                    // Let the limit deciede if we should continue or not...
                    return effectiveLimit.shouldContinue();
                }, scrollResponse);
            } finally {
                client.closeScroll(scrollResponse.getString(RESPONSE_SCROLL_ID));
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
    private JSONObject executeScroll(Function<E, Boolean> handler, JSONObject firstResponse) {
        long lastScroll = 0;
        JSONObject scrollResponse = firstResponse;
        while (true) {
            // we keep ob executing queries until es returns an empty list of results...
            JSONArray hits = scrollResponse.getJSONObject("hits").getJSONArray("hits");
            if (hits.isEmpty()) {
                return scrollResponse;
            }

            for (Object obj : hits) {
                if (!handler.apply((E) Elastic.make(descriptor, (JSONObject) obj))) {
                    return scrollResponse;
                }
            }

            lastScroll = performScrollMonitoring(lastScroll);
            scrollResponse = client.continueScroll(SCROLL_TTL_SECONDS, scrollResponse.getString(RESPONSE_SCROLL_ID));
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
    public void delete() {
        iterateAll(elastic::delete);
    }

    @Override
    public void truncate() {
        List<String> indices = determineIndices();
        if (indices.isEmpty()) {
            return;
        }

        elastic.getLowLevelClient()
               .deleteByQuery(indices, elastic.determineTypeName(descriptor), routing, buildSimplePayload());
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
