/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.alibaba.fastjson2.JSONObject;
import sirius.kernel.commons.Explain;

/**
 * Represents a bucket of an aggregation result.
 */
public class Bucket {

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
