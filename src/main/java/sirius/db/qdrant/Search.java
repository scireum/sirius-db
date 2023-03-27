/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.qdrant;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import sirius.kernel.commons.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a search query to be executed against a qdrant collection.
 */
public class Search {

    private final QdrantDatabase qdrantDatabase;
    private final String collection;
    private final float[] vector;
    private List<Tuple<String, Object>> mustFilters;
    private List<Tuple<String, Object>> mustNotFilters;

    protected Search(QdrantDatabase qdrantDatabase, String collection, float[] vector) {
        this.qdrantDatabase = qdrantDatabase;
        this.collection = collection;
        this.vector = vector.clone();
    }

    /**
     * Sepcifies a payload filter which must match.
     *
     * @param key   the name of the field to match
     * @param value the value to match
     * @return the search itself for fluent method calls
     */
    public Search mustMatch(String key, Object value) {
        if (mustFilters == null) {
            mustFilters = new ArrayList<>();
        }

        mustFilters.add(Tuple.create(key, value));
        return this;
    }

    /**
     * Specifies a payload filter which must not match.
     *
     * @param key   the name of the field to match
     * @param value the value to "not match"
     * @return the search itself for fluent method calls
     */
    public Search mustNotMatch(String key, Object value) {
        if (mustNotFilters == null) {
            mustNotFilters = new ArrayList<>();
        }

        mustNotFilters.add(Tuple.create(key, value));
        return this;
    }

    /**
     * Executes the search and returns the value of the requested payload field.
     *
     * @param fieldType  the datatype of the field to return
     * @param field      the name of the field to return
     * @param maxResults the maximum number of results to return
     * @param <T>        the generic type of the field to return
     * @return the matched points ordered by relevance
     */
    public <T> List<T> queryPayload(Class<T> fieldType, String field, int maxResults) {
        JSONObject query = new JSONObject();
        query.put("vector", vector);
        query.put("with_payload", new JSONArray().fluentAdd(field));
        query.put("limit", maxResults);

        if (mustFilters != null || mustNotFilters != null) {
            JSONObject filterObject = new JSONObject();
            if (mustFilters != null) {
                filterObject.put("must", buildConstraints(mustFilters));
            }
            if (mustNotFilters != null) {
                filterObject.put("must_not", buildConstraints(mustNotFilters));
            }
        }

        JSONObject response = qdrantDatabase.execute(QdrantDatabase.Method.POST,
                                                     QdrantDatabase.URI_PREFIX_COLLECTIONS + collection + "/points/search",
                                                     query);
        JSONArray points = response.getJSONArray("result");

        List<T> payloadData = new ArrayList<>(points.size());
        for (JSONObject point : points.toJavaList(JSONObject.class)) {
            payloadData.add(fieldType.cast(point.getJSONObject("payload").get(field)));
        }

        return payloadData;
    }

    private JSONArray buildConstraints(List<Tuple<String, Object>> filters) {
        JSONArray result = new JSONArray();
        filters.stream().map(filter -> {
            JSONObject filterJson = new JSONObject();
            filterJson.put("key", filter.getFirst());
            filterJson.put("match", new JSONObject().fluentPut("value", filter.getSecond()));
            return filterJson;
        }).forEach(result::add);

        return result;
    }
}
