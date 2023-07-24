/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.qdrant;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import sirius.kernel.commons.Json;
import sirius.kernel.commons.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
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
     * Specifies a payload filter which must match.
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
     * @param maxResults    the maximum number of results to return
     * @param payloadFields the field to pull from the payload
     * @return the matched points ordered by relevance
     */
    public List<Match> execute(int maxResults, String... payloadFields) {
        ArrayNode payloadFieldArray = Json.createArray();
        Arrays.stream(payloadFields).forEach(payloadFieldArray::add);

        ObjectNode query = Json.createObject();
        query.putPOJO("vector", vector);
        query.set("with_payload", payloadFieldArray);
        query.put("limit", maxResults);

        if (mustFilters != null || mustNotFilters != null) {
            ObjectNode filterObject = Json.createObject();
            if (mustFilters != null) {
                filterObject.set("must", buildConstraints(mustFilters));
            }
            if (mustNotFilters != null) {
                filterObject.set("must_not", buildConstraints(mustNotFilters));
            }
        }

        ObjectNode response = qdrantDatabase.execute(QdrantDatabase.Method.POST,
                                                     QdrantDatabase.URI_PREFIX_COLLECTIONS
                                                     + collection
                                                     + "/points/search",
                                                     query);
        return Json.streamEntries(Json.getArray(response, "result"))
                   .map(ObjectNode.class::cast)
                   .map(Match::new)
                   .toList();
    }

    private ArrayNode buildConstraints(List<Tuple<String, Object>> filters) {
        ArrayNode result = Json.createArray();
        filters.stream().map(filter -> {
            ObjectNode filterJson = Json.createObject();
            filterJson.put("key", filter.getFirst());
            filterJson.set("match", Json.createObject().putPOJO("value", filter.getSecond()));
            return filterJson;
        }).forEach(result::add);

        return result;
    }
}
