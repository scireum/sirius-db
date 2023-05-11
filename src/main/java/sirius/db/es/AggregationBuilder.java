/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import sirius.db.es.constraints.ElasticConstraint;
import sirius.db.mixing.Mapping;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Json;
import sirius.kernel.commons.Strings;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
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
     * Contains the field name of the precision_threshold, which controls how exact a cardinality aggregation is.
     */
    public static final String PRECISION_THRESHOLD = "precision_threshold";

    /**
     * Type string for value count aggregations
     *
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-valuecount-aggregation.html">
     * ElasticSearch reference page for value count aggregations</a>
     */
    public static final String VALUE_COUNT = "value_count";

    /**
     * Type string for composite aggregations
     *
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html">
     * ElasticSearch reference page for composite aggregations</a>
     */
    public static final String COMPOSITE = "composite";

    /**
     * Type string for histogram aggregations
     *
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-histogram-aggregation.html">
     * ElasticSearch reference page for histogram aggregations</a>
     */
    public static final String HISTOGRAM = "histogram";

    private static final String NESTED_PATH = "path";
    private static final String AGGREGATIONS = "aggs";
    private static final String FIELD = "field";
    private static final String SIZE = "size";
    private static final String OFFSET = "offset";
    private static final String INTERVAL = "interval";
    private static final String MIN_DOC_COUNT = "min_doc_count";
    private static final String AFTER = "after";

    private String name;
    private String type;
    private ObjectNode body = Json.createObject();
    private String path;
    private List<AggregationBuilder> subAggregations;
    private List<AggregationBuilder> sourceAggregations;

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
     * Creates a new composite aggregation builder.
     *
     * @param name the name of the aggregation
     * @return the builder itself for fluent method calls
     */
    public static AggregationBuilder createComposite(String name) {
        return new AggregationBuilder(COMPOSITE, null, name);
    }

    /**
     * Creates a new histogram aggregation builder.
     *
     * @param name        the name of the aggregation
     * @param field       the field to build the histogram for
     * @param offset      the offset (first bucket value) to use
     * @param interval    the interval (bucket size) to use
     * @param minDocCount determines the minimal document count for a bucket to be part of the result
     * @return the builder itself for fluent method calls
     */
    public static AggregationBuilder createHistogram(String name,
                                                     Mapping field,
                                                     double offset,
                                                     double interval,
                                                     int minDocCount) {
        return new AggregationBuilder(HISTOGRAM, null, name).field(field)
                                                            .addBodyParameter(OFFSET, offset)
                                                            .addBodyParameter(INTERVAL, interval)
                                                            .addBodyParameter(MIN_DOC_COUNT, minDocCount);
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
        if (value instanceof JsonNode jsonNode) {
            this.body.set(name, jsonNode);
        } else {
            this.body.putPOJO(name, value);
        }
        return this;
    }

    /**
     * Adds the given aggregation as source aggregation to a {@link #COMPOSITE} aggegation.
     *
     * @param sourceAggregation the source aggregation to add.
     * @return the aggregation builder itself for fluent methdo calls
     */
    public AggregationBuilder addSourceAggregation(AggregationBuilder sourceAggregation) {
        if (sourceAggregations == null) {
            sourceAggregations = new ArrayList<>();
        }

        sourceAggregations.add(sourceAggregation);

        return this;
    }

    /**
     * Adds a terms aggregation for the given field as a source to a {@link #COMPOSITE} aggegation.
     *
     * @param field the field to aggregate the terms from
     * @return the aggregation builder itself for fluent methdo calls
     */
    public AggregationBuilder addTermSourceAggregation(Mapping field) {
        return addSourceAggregation(AggregationBuilder.createTerms(field).size(-1));
    }

    /**
     * Installs an "after key" which is used to perform pagination.
     * <p>
     * The key is produced by {@link AggregationResult#getCompoundAfterKey()} and can be passed into this method
     * to access the "next page".
     *
     * @param afterKey the compond after key to parse and install
     * @return the aggregation builder itself for fluent methdo calls
     */
    public AggregationBuilder withCompoundAfterKey(@Nullable String afterKey) {
        if (Strings.isFilled(afterKey)) {
            ObjectNode afterKeyObject =
                    Json.parseObject(new String(Base64.getDecoder().decode(afterKey), StandardCharsets.UTF_8));
            addBodyParameter(AFTER, afterKeyObject);
        }

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
    public AggregationBuilder withBody(ObjectNode body) {
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
     * <p>
     * Use a negative or <tt>0</tt> as size to suppress the size parameter.
     *
     * @param size the size to set
     * @return the builder itself for fluent method calls
     */
    public AggregationBuilder size(int size) {

        if (size > 0) {
            return addBodyParameter(SIZE, size);
        } else {
            this.body.remove(SIZE);
            return this;
        }
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
        if (subAggregations == null) {
            return Collections.emptyList();
        }

        return Collections.unmodifiableList(subAggregations);
    }

    /**
     * Determines if this aggregation has sub-aggregations
     *
     * @return <tt>true</tt> if there are sub aggregations, <tt>false</tt> otherwise
     */
    public boolean hasSubAggregations() {
        return subAggregations != null && !subAggregations.isEmpty();
    }

    /**
     * Generates the json of the current builder.
     *
     * @return the json representation of the current builder.
     */
    public ObjectNode build() {
        ObjectNode builder = Json.createObject();

        if (Strings.isFilled(path)) {
            builder.set(NESTED, Json.createObject().put(NESTED_PATH, path));
        } else {
            builder.set(type, body);
        }

        if (subAggregations != null) {
            ObjectNode subAggs = Json.createObject();
            subAggregations.forEach(subAggregation -> subAggs.set(subAggregation.getName(), subAggregation.build()));
            builder.set(AGGREGATIONS, subAggs);
        }

        if (sourceAggregations != null) {
            ArrayNode sourceAggs = Json.createArray();
            sourceAggregations.forEach(sourceAgg -> sourceAggs.add(Json.createObject()
                                                                       .set(sourceAgg.getName(), sourceAgg.build())));
            body.set("sources", sourceAggs);
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
