/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.alibaba.fastjson.JSONObject;
import sirius.db.es.constraints.ElasticConstraint;
import sirius.db.mixing.Mapping;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Strings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Helper class which generates aggregations for elasticsearch which can be used via {@link ElasticQuery#addAggregation(AggregationBuilder)}.
 */
public class AggregationBuilder {

    /**
     * Type string for nested aggregations
     *
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-nested-aggregation.html">
     * ElasticSearch reference page for nested aggregations</a>
     */
    public static final String NESTED = "nested";

    /**
     * Type string for filter aggregations
     *
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-filter-aggregation.html">
     * ElasticSearch reference page for filter aggregations</a>
     */
    public static final String FILTER = "filter";

    /**
     * Type string for terms aggregations
     *
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html">
     * ElasticSearch reference page for terms aggregations</a>
     */
    public static final String TERMS = "terms";

    /**
     * Contains the default number of buckets being collected and reported for an aggregation.
     */
    public static final int DEFAULT_TERM_AGGREGATION_BUCKET_COUNT = 25;

    /**
     * Type string for min aggregations
     *
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-min-aggregation.html">
     * ElasticSearch reference page for min aggregations</a>
     */
    public static final String MIN = "min";

    /**
     * Type string for max aggregations
     *
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-max-aggregation.html">
     * ElasticSearch reference page for max aggregations</a>
     */
    public static final String MAX = "max";

    /**
     * Type string for cardinality aggregations
     *
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-cardinality-aggregation.html">
     * ElasticSearch reference page for cardinality aggregations</a>
     */
    public static final String CARDINALITY = "cardinality";

    /**
     * Type string for value count aggregations
     *
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-valuecount-aggregation.html">
     * ElasticSearch reference page for value count aggregations</a>
     */
    public static final String VALUE_COUNT = "value_count";

    private static final String NESTED_PATH = "path";
    private static final String AGGREGATIONS = "aggs";
    private static final String FIELD = "field";
    private static final String SIZE = "size";

    private String name;
    private String type;
    private JSONObject body = new JSONObject();
    private String path;
    private List<AggregationBuilder> subAggregations;

    private AggregationBuilder(String type, String path, String name) {
        this.path = path;
        this.type = type;
        this.name = name;
    }

    /**
     * Creates a new aggregation builder.
     *
     * @param type the type of the aggregation.
     * @param name the name of the aggregation
     * @return the builder itself for fluent method calls
     */
    public static AggregationBuilder create(String type, String name) {
        return new AggregationBuilder(type, null, name);
    }

    /**
     * Creates a new term aggregation builder.
     *
     * @param field the field to aggregate on
     * @return the builder itself for fluent method calls
     */
    public static AggregationBuilder createTerms(Mapping field) {
        return new AggregationBuilder(TERMS, null, field.getName()).field(field)
                                                                   .size(DEFAULT_TERM_AGGREGATION_BUCKET_COUNT);
    }

    /**
     * Creates a new cardinality aggregation builder.
     *
     * @param name  the name of the aggregation
     * @param field the field to aggregate on
     * @return the builder itself for fluent method calls
     */
    public static AggregationBuilder createCardinality(String name, Mapping field) {
        return new AggregationBuilder(CARDINALITY, null, name).field(field);
    }

    /**
     * Creates a new value count aggregation builder.
     *
     * @param name  the name of the aggregation
     * @param field the field to aggregate on
     * @return the builder itself for fluent method calls
     */
    public static AggregationBuilder createValueCount(String name, Mapping field) {
        return new AggregationBuilder(VALUE_COUNT, null, name).field(field);
    }

    /**
     * Creates a new aggregation builder for nested fields.
     *
     * @param path the path of the nested mapping
     * @param name the name of the aggregation
     * @return the builder itself for fluent method calls
     */
    public static AggregationBuilder createNested(Mapping path, String name) {
        return new AggregationBuilder(null, path.getName(), name);
    }

    /**
     * Creates a new aggregation builder for a filter aggregation.
     *
     * @param name   the name of the aggregation
     * @param filter the constraint to filter on
     * @return the builder itself for fluent method calls
     */
    public static AggregationBuilder createFiltered(String name, ElasticConstraint filter) {
        return new AggregationBuilder(FILTER, null, name).withBody(filter.toJSON());
    }

    /**
     * Adds a parameter to the body of the aggregation.
     *
     * @param name  the name of the parameter
     * @param value the value of the parameter
     * @return the builder itself for fluent method calls
     */
    public AggregationBuilder addBodyParameter(String name, Object value) {
        this.body.put(name, value);
        return this;
    }

    /**
     * Sets the body of the aggregation.
     *
     * @param body the body to use
     * @return the builder itself for fluent method calls
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Explain("We do not create an extra copy here for performance reasons, as the scope is quite limited")
    public AggregationBuilder withBody(JSONObject body) {
        this.body = body;
        return this;
    }

    /**
     * Adds the field parameter to the aggregation.
     *
     * @param field the field to add
     * @return the builder itself for fluent method calls
     */
    public AggregationBuilder field(Mapping field) {
        return addBodyParameter(FIELD, field.toString());
    }

    /**
     * Adds the size parameter to the aggregation.
     *
     * @param size the size to set
     * @return the builder itself for fluent method calls
     */
    public AggregationBuilder size(int size) {
        return addBodyParameter(SIZE, size);
    }

    /**
     * Adds  a subaggregation to the builder.
     *
     * @param subAggregation the builder for the subaggregation
     * @return the builder itself for fluent method calls
     */
    public AggregationBuilder addSubAggregation(AggregationBuilder subAggregation) {
        if (subAggregations == null) {
            subAggregations = new ArrayList<>();
        }

        subAggregations.add(subAggregation);

        return this;
    }

    /**
     * Returns the name of the aggregation.
     *
     * @return the name of the aggregation.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the list of sub aggregations of this aggregation.
     *
     * @return the list of sub aggregations
     */
    public List<AggregationBuilder> getSubAggregations() {
        return Collections.unmodifiableList(subAggregations);
    }

    /**
     * Determines if this aggregation has sub-aggregations
     *
     * @return <tt>true</tt> if there are sub aggregations, <tt>false</tt> otherwise
     */
    public boolean hasSubAggregations() {
        return !subAggregations.isEmpty();
    }

    /**
     * Generates the json of the current builder.
     *
     * @return the json representation of the current builder.
     */
    public JSONObject build() {
        JSONObject builder = new JSONObject();

        if (Strings.isFilled(path)) {
            builder.put(NESTED, new JSONObject().fluentPut(NESTED_PATH, path));
        } else {
            builder.put(type, body);
        }

        if (subAggregations != null) {
            JSONObject subAggs = new JSONObject();
            subAggregations.forEach(subAggregation -> subAggs.put(subAggregation.getName(), subAggregation.build()));
            builder.put(AGGREGATIONS, subAggs);
        }

        return builder;
    }

    /**
     * Generates a copy of this aggregation builder to support {@link ElasticQuery#copy()}.
     *
     * @return a copy of this builder
     */
    public AggregationBuilder copy() {
        AggregationBuilder copy = new AggregationBuilder(type, path, name);
        copy.body = Elastic.copyJSON(this.body);
        if (subAggregations != null) {
            copy.subAggregations =
                    this.subAggregations.stream().map(AggregationBuilder::copy).collect(Collectors.toList());
        }

        return copy;
    }
}
