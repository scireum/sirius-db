/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.alibaba.fastjson2.JSONObject;
import sirius.db.es.constraints.ElasticConstraint;
import sirius.db.mixing.Mapping;

/**
 * Helper class which generates sorts for elasticsearch which can be used via {@link ElasticQuery#sort(SortBuilder)}.
 */
public class SortBuilder {

    /**
     * Defines the order when sorting.
     */
    public enum Order {

        /**
         * Ascending order
         */
        ASC,

        /**
         * Descending order
         */
        DESC
    }

    /**
     * Controls on which value a multi-valued field is sorted.
     */
    public enum Mode {

        /**
         * The lowest value
         */
        MIN,

        /**
         * The highest value
         */
        MAX,

        /**
         * The sum of all values
         */
        SUM,

        /**
         * The average of all values
         */
        AVG,

        /**
         * The median of all values
         */
        MEDIAN
    }

    private String field;
    private JSONObject body = new JSONObject();

    private SortBuilder(String field) {
        this.field = field;
    }

    /**
     * Creates a new sort builder.
     *
     * @param field the field to sort on
     * @return the builder itself for fluent method calls
     */
    public static SortBuilder on(Mapping field) {
        return new SortBuilder(field.toString());
    }

    /**
     * Creates a new sort builder for geo distance sorting.
     *
     * @return the builder itself for fluent method calls
     */
    public static SortBuilder onGeoDistance() {
        return new SortBuilder("_geo_distance");
    }

    /**
     * Creates a new sort builder for script based sorting.
     *
     * @return the builder itself for fluent method calls
     */
    public static SortBuilder onScript() {
        return new SortBuilder("_script");
    }

    /**
     * Adds a parameter to the body of the sort.
     *
     * @param name  the name of the parameter
     * @param value the value of the parameter
     * @return the builder itself for fluent method calls
     */
    public SortBuilder addBodyParameter(String name, Object value) {
        this.body.put(name, value);
        return this;
    }

    /**
     * Adds the order parameter to the sort.
     *
     * @param order the {@link Order} option to use
     * @return the builder itself for fluent method calls
     */
    public SortBuilder order(Order order) {
        return addBodyParameter("order", order.toString());
    }

    /**
     * Adds the mode parameter to the sort.
     *
     * @param mode the {@link Mode} option to use
     * @return the builder itself for fluent method calls
     */
    public SortBuilder mode(Mode mode) {
        return addBodyParameter("mode", mode.toString());
    }

    /**
     * Sort on a nested object.
     *
     * @param path   the nested object to sort on
     * @param filter the filter to check wether objects should be taken into account when sorting
     * @return the builder itself for fluent method calls
     */
    public SortBuilder nested(Mapping path, ElasticConstraint filter) {
        return addBodyParameter("nested",
                                new JSONObject().fluentPut("path", path.toString())
                                                .fluentPut("filter", filter.toJSON()));
    }

    /**
     * Generates the json of the current builder.
     *
     * @return the json representation of the current builder.
     */
    public JSONObject build() {
        JSONObject builder = new JSONObject();

        builder.put(field, body);

        return builder;
    }
}
