/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.filter;

import com.alibaba.fastjson.JSONObject;
import sirius.db.mixing.Mapping;

/**
 * Represents a constraint which checks that the given field is not empty
 */
public class Filled extends BaseFilter {

    private String field;
    private Float boost;

    /**
     * Creates a filter which ensures that the given field isn't empty.
     *
     * @param field the field to check
     */
    public Filled(String field) {
        this.field = field;
    }

    /**
     * Creates a filter which ensures that the given field isn't empty.
     *
     * @param field the field to check
     */
    public Filled(Mapping field) {
        this.field = field.toString();
    }

    /**
     * Sets the boost value that should be used.
     *
     * @param boost the boost value
     * @return the constraint itself for fluent method calls
     */
    public Filled withBoost(Float boost) {
        this.boost = boost;
        return this;
    }

    @Override
    public JSONObject toJSON() {
        JSONObject innerQuery = new JSONObject().fluentPut("field", this.field);
        if (boost != null) {
            innerQuery.put("boost", boost);
        }
        return new JSONObject().fluentPut("exists", innerQuery);
    }
}
