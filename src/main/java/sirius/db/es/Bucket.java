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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Represents a bucket of an aggregation result.
 */
public class Bucket {

    private static final String KEY_BUCKETS = "buckets";
    private static final String KEY_KEY = "key";
    private static final String KEY_DOC_COUNT = "doc_count";

    private final String key;
    private JSONObject data;

    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Explain("This data is normally read only and performing a deep copy is not worth the overhead.")
    protected Bucket(String key, JSONObject data) {
        this.key = key;
        this.data = data;
    }

    /**
     * Creates a list of buckets from the given aggregation.
     *
     * @param aggregation the aggregation as {@link JSONObject} to read the buckets from
     * @return a list of buckets
     * @deprecated use {@link AggregationResult#forEachBucket(Consumer)} etc.
     */
    @Deprecated(since = "2021/07/01")
    public static List<Bucket> fromAggregation(JSONObject aggregation) {
        List<Bucket> result = new ArrayList<>();

        if (aggregation == null) {
            return result;
        }

        Object buckets = aggregation.get(KEY_BUCKETS);

        if (buckets instanceof JSONArray) {
            for (Object bucket : (JSONArray) buckets) {
                result.add(new Bucket(null, (JSONObject) bucket));
            }
        } else if (buckets instanceof JSONObject) {
            for (Map.Entry<String, Object> entry : ((JSONObject) buckets).entrySet()) {
                result.add(new Bucket(entry.getKey(), (JSONObject) entry.getValue()));
            }
        }

        return result;
    }

    /**
     * Returns the key of this bucket.
     *
     * @return the key
     */
    public String getKey() {
        if (key != null) {
            return key;
        }

        return data.getString(KEY_KEY);
    }

    /**
     * Returns an inner key field in case of a composite key.
     * <p>
     * Composite keys are commonly created when using {@link AggregationBuilder#COMPOSITE composite} aggregations with
     * multiple sources.
     *
     * @param name the name of the source aggregation which value is to be fetched
     * @return the aggregated source value
     */
    public String getKey(String name) {
        return data.getJSONObject(KEY_KEY).getString(name);
    }

    /**
     * Returns the doc count of this bucket.
     *
     * @return the doc count
     */
    public int getDocCount() {
        return data.getIntValue(KEY_DOC_COUNT);
    }

    /**
     * Returns the raw {@link JSONObject} of this bucket.
     *
     * @return the raw {@link JSONObject}
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Explain("This data is normally read only and performing a deep copy is not worth the overhead.")
    public JSONObject getJSONObject() {
        return data;
    }

    /**
     * Returns the inner aggregation result with the given name.
     *
     * @param name the name of the sub aggregation
     * @return the collected result for the given sub aggregation
     */
    public AggregationResult getSubAggregation(String name) {
        return AggregationResult.of(data.getJSONObject(name));
    }
}
