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
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Represents the result of an aggregation executed by Elasticsearch.
 */
public class AggregationResult {

    private static final String KEY_VALUE = "value";
    private static final String KEY_BUCKETS = "buckets";
    private static final AggregationResult EMPTY = new AggregationResult(new JSONObject());

    private JSONObject data;

    /**
     * Uses {@link #of(JSONObject)} to generate a new instance while handling <tt>null</tt> values for data gracefully.
     *
     * @param data the raw JSON data to wrap
     */
    private AggregationResult(JSONObject data) {
        this.data = data;
    }

    /**
     * Creates a new result of the given data.
     * <p>
     * Most probably this method shouldn't be called directly but {@link ElasticQuery#getAggregation(String)} or
     * {@link Bucket#getSubAggregation(String)} or {@link AggregationResult#getSubAggregation(String)} should be used.
     *
     * @param data the JSON data representing the aggregation. This also handles <tt>null</tt> gracefully.
     * @return a result object representing the given raw JSON
     */
    @Nonnull
    public static AggregationResult of(@Nullable JSONObject data) {
        if (data == null) {
            return EMPTY;
        }

        return new AggregationResult(data);
    }

    /**
     * Invokes the given consumer for each bucket in the given multi bucket aggregation.
     *
     * @param bucketConsumer the consumer to be invoked for each bucket in the aggregation
     */
    public void forEachBucket(Consumer<Bucket> bucketConsumer) {
        Object buckets = data.get(KEY_BUCKETS);

        if (buckets instanceof JSONArray) {
            for (Object bucket : (JSONArray) buckets) {
                bucketConsumer.accept(new Bucket((JSONObject) bucket));
            }
        }
    }

    /**
     * Extracts and returns the list of buckets in this aggregation result.
     *
     * @return the list of aggregated buckets
     */
    public List<Bucket> getBuckets() {
        List<Bucket> result = new ArrayList<>();
        forEachBucket(result::add);
        return result;
    }

    /**
     * Wraps a bucket result into a list of keys and their document counts.
     *
     * @return the direct list of key and document count for each bucket in this result
     */
    public List<Tuple<String, Integer>> getTermCounts() {
        List<Tuple<String, Integer>> result = new ArrayList<>();
        forEachBucket(bucket -> result.add(Tuple.create(bucket.getKey(), bucket.getDocCount())));
        return result;
    }

    /**
     * Tries to obtain and return the first bucket in this result.
     * <p>
     * This is mainly used for single bucket aggregations or for sub aggregations which are expected to
     * have only one result bucket.
     *
     * @return the bucket in this result or an empty optional if there is no bucket
     */
    public Optional<Bucket> getFirstBucket() {
        Object buckets = data.get(KEY_BUCKETS);

        if (buckets instanceof JSONArray) {
            return Optional.of(new Bucket((JSONObject) ((JSONArray) buckets).get(0)));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Returns the cardinality value if this is the result of a {@link AggregationBuilder#CARDINALITY} aggregation.
     *
     * @return the cardinality computed by this aggregation
     */
    public Optional<Integer> getCardinality() {
        return Value.of(data.get(KEY_VALUE)).asOptionalInt();
    }

    /**
     * Returns the value count if this is the result of a {@link AggregationBuilder#VALUE_COUNT} aggregation.
     *
     * @return the number of (non-distinct) values computed by this aggregation
     */
    public Optional<Integer> getValueCount() {
        return Value.of(data.get(KEY_VALUE)).asOptionalInt();
    }

    /**
     * Returns the sub aggregation embedded in this aggregation.
     *
     * @param name the name of the sub aggregation to fetch
     * @return the sub aggregation with the given name
     */
    @Nonnull
    public AggregationResult getSubAggregation(String name) {
        return AggregationResult.of(data.getJSONObject(name));
    }

    /**
     * Returns the raw {@link JSONObject} of this aggregation result.
     *
     * @return the raw {@link JSONObject}
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Explain("This data is normally read only and performing a deep copy is not worth the overhead.")
    public JSONObject getJSONObject() {
        return data;
    }
}
