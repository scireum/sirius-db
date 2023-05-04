/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.fasterxml.jackson.databind.node.ObjectNode;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Json;

/**
 * Represents a bucket of an aggregation result.
 */
public class Bucket {

    private static final String KEY_KEY = "key";
    private static final String KEY_DOC_COUNT = "doc_count";

    private final String key;
    private ObjectNode data;

    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Explain("This data is normally read only and performing a deep copy is not worth the overhead.")
    protected Bucket(String key, ObjectNode data) {
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

        return data.get(KEY_KEY).asText();
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
        return data.get(KEY_KEY).get(name).asText();
    }

    /**
     * Returns the doc count of this bucket.
     *
     * @return the doc count
     */
    public int getDocCount() {
        return data.get(KEY_DOC_COUNT).asInt();
    }

    /**
     * Returns the raw {@link ObjectNode} of this bucket.
     *
     * @return the raw {@link ObjectNode}
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Explain("This data is normally read only and performing a deep copy is not worth the overhead.")
    public ObjectNode withObject() {
        return data;
    }

    /**
     * Returns the inner aggregation result with the given name.
     *
     * @param name the name of the sub aggregation
     * @return the collected result for the given sub aggregation
     */
    public AggregationResult getSubAggregation(String name) {
        return AggregationResult.of(Json.getObject(data, name));
    }
}
