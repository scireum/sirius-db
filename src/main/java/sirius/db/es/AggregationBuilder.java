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
import sirius.kernel.commons.Strings;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper class which generates aggregations for elasticsearch which can be used via {@link ElasticQuery#addAggregation(AggregationBuilder)}.
 */
public class AggregationBuilder {

    public static final String NESTED = "nested";
    public static final String FILTER = "filter";
    public static final String TERMS = "terms";
    public static final String MIN = "min";
    public static final String MAX = "max";
    public static final String CARDINALITY = "cardinality";

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
}
