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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Represents a bucket of an aggregation result.
 */
public class Bucket {

    private static final String KEY_BUCKETS = "buckets";
    private static final String KEY_KEY = "key";
    private static final String KEY_DOC_COUNT = "doc_count";

    private String key;
    private JSONObject jsonObject;

    private Bucket(JSONObject jsonObject) {
        this(jsonObject.getString(KEY_KEY), jsonObject);
    }

    private Bucket(String key, JSONObject jsonObject) {
        this.key = key;
        this.jsonObject = jsonObject;
    }

    /**
     * Creates a list of buckets from the given aggregation.
     *
     * @param aggregation the aggregation as {@link JSONObject} to read the buckets from
     * @return a list of buckets
     */
    public static List<Bucket> fromAggregation(JSONObject aggregation) {
        List<Bucket> result = new ArrayList<>();

        if (aggregation == null) {
            return result;
        }

        Object buckets = aggregation.get(KEY_BUCKETS);

        if (buckets instanceof JSONArray) {
            for (Object bucket : (JSONArray) buckets) {
                result.add(new Bucket((JSONObject) bucket));
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
        return key;
    }

    /**
     * Returns the doc count of this bucket.
     *
     * @return the doc count
     */
    public int getDocCount() {
        return jsonObject.getIntValue(KEY_DOC_COUNT);
    }

    /**
     * Returns the raw {@link JSONObject} of this bucket.
     *
     * @return the raw {@link JSONObject}
     */
    public JSONObject getJSONObject() {
        return jsonObject;
    }
}
