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
import sirius.db.es.filter.BoolQueryBuilder;
import sirius.db.es.filter.FieldEqual;
import sirius.db.es.filter.Filter;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.Query;
import sirius.kernel.async.ExecutionPoint;
import sirius.kernel.async.TaskContext;
import sirius.kernel.commons.Limit;
import sirius.kernel.commons.RateLimit;
import sirius.kernel.commons.Tuple;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Provides a fluent query API for Elasticsearch.
 *
 * @param <E> the type of entities to be queried
 */
public class ElasticQuery<E extends ElasticEntity> extends Query<ElasticQuery<E>, E> {

    private static final int SCROLL_TTL_SECONDS = 60 * 5;
    private static final int MAX_SCROLL_RESULTS_FOR_SINGLE_SHARD = 50;
    private static final int MAX_SCROLL_RESULTS_PER_SHARD = 10;
    private static final String RESPONSE_SCROLL_ID = "_scroll_id";
    private static final String KEY_DOC_ID = "_doc";

    @Part
    private static Elastic elastic;

    @Part
    private static IndexMappings indexMappings;

    private final LowLevelClient client;

    private BoolQueryBuilder queryBuilder;

    private List<Tuple<String, JSONObject>> aggregations;

    private List<JSONObject> postFilters;

    private String collapseBy;
    private List<JSONObject> collapseByInnerHits;

    private List<JSONObject> sorts;

    private List<Integer> years;

    private String routing;
    private boolean unrouted;

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
    public ElasticQuery<E> must(Filter filter) {
        return must(filter.toJSON());
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

    public ElasticQuery<E> mustNot(Filter filter) {
        return mustNot(filter.toJSON());
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

    /**
     * Adds a FILTER constraint to the query.
     *
     * @param filter the filter to add
     * @return the query itself for fluent method calls
     * @see BoolQueryBuilder#filter(Filter)
     */
    public ElasticQuery<E> filter(Filter filter) {
        return filter(filter.toJSON());
    }

    /**
     * Adds a {@link FieldEqual} filter for the given field and value as FILTER to the query.
     *
     * @param field the field to filter on
     * @param value the value to filter on
     * @return the query itself for fluent method calls
     */
    @Override
    public ElasticQuery<E> eq(Mapping field, Object value) {
        return filter(new FieldEqual(field, value));
    }

    /**
     * Adds a {@link FieldEqual} filter for the given field and value as FILTER to the query.
     * <p>
     * If the given <tt>value</tt> if <tt>null</tt>, no filter will be added.
     *
     * @param field the field to filter on
     * @param value the value to filter on
     * @return the query itself for fluent method calls
     */
    @Override
    public ElasticQuery<E> eqIgnoreNull(Mapping field, Object value) {
        return filter(new FieldEqual(field, value).ignoreNull());
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
        if (VersionedEntity.class.isAssignableFrom(descriptor.getType())) {
            payload.put("version", true);
        }

        if (queryBuilder != null) {
            payload.put("query", queryBuilder.toJSON());
        }

        if (sorts != null && !sorts.isEmpty()) {
            payload.put("sort", sorts);
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
            payload.put("query", queryBuilder.toJSON());
        }

        return payload;
    }

    @Override
    public long count() {
        if (skip > 0 || limit > 0) {
            Elastic.LOG.WARN("COUNT queries support neither skip nor limit: %s\n%s", this, ExecutionPoint.snapshot());
        }

        List<String> indices = determineIndices();

        if (indices.isEmpty()) {
            return 0;
        }

        JSONObject response =
                client.count(indices, elastic.determineTypeName(descriptor), routing, buildSimplePayload());
        return response.getLong("count");
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

        List<String> indices = determineIndices();

        if (indices.isEmpty()) {
            return false;
        }

        JSONObject response =
                client.exists(indices, elastic.determineTypeName(descriptor), routing, buildSimplePayload());
        return response.getJSONObject("hits").getInteger("total") >= 1;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void iterate(Function<E, Boolean> handler) {
        if (limit == 0 || limit > MAX_LIST_SIZE) {
            scroll(handler);
            return;
        }

        List<String> indices = determineIndices();
        if (indices.isEmpty()) {
            return;
        }

        JSONObject response =
                client.search(indices, elastic.determineTypeName(descriptor), routing, skip, limit, buildPayload());
        for (Object obj : response.getJSONObject("hits").getJSONArray("hits")) {
            if (!handler.apply((E) Elastic.make(descriptor, (JSONObject) obj))) {
                return;
            }
        }
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

            List<String> indices = determineIndices();
            if (indices.isEmpty()) {
                return;
            }

            JSONObject response = client.createScroll(indices,
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
                response = executeScroll(entity -> {
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
                }, response);
            } finally {
                client.closeScroll(response.getString(RESPONSE_SCROLL_ID));
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
        JSONObject response = firstResponse;
        while (true) {
            // we keep ob executing queries until es returns an empty list of results...
            JSONArray hits = response.getJSONObject("hits").getJSONArray("hits");
            if (hits.isEmpty()) {
                return response;
            }

            for (Object obj : hits) {
                if (!handler.apply((E) Elastic.make(descriptor, (JSONObject) obj))) {
                    return response;
                }
            }

            lastScroll = performScrollMonitoring(lastScroll);
            response = client.continueScroll(SCROLL_TTL_SECONDS, response.getString(RESPONSE_SCROLL_ID));
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
    public String toString() {
        return descriptor.getType() + ": " + buildPayload();
    }
}
