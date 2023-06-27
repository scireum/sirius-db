/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.fasterxml.jackson.databind.node.ObjectNode;
import sirius.db.es.constraints.ElasticConstraint;
import sirius.db.mixing.Mapping;
import sirius.kernel.commons.Json;

/**
 * Helper class which generates sorts for elasticsearch which can be used via {@link ElasticQuery#order(SortBuilder)}.
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
    private ObjectNode body = Json.createObject();

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
        this.body.putPOJO(name, value);
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
                                Json.createObject().put("path", path.toString()).set("filter", filter.toJSON()));
    }

    /**
     * Generates the json of the current builder.
     *
     * @return the json representation of the current builder.
     */
    public ObjectNode build() {
        ObjectNode builder = Json.createObject();

        builder.set(field, body);

        return builder;
    }
}
