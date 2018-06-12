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
 * Represents a constraint which verifies that a given field is empty.
 */
public class NotFilled extends BaseFilter {

    private String field;
    private Float boost = null;

    /**
     * Creates a filter which ensures that the given field is empty.
     *
     * @param field the field to check
     */
    public NotFilled(String field) {
        this.field = field;
    }

    /**
     * Creates a filter which ensures that the given field is empty.
     *
     * @param field the field to check
     */
    public NotFilled(Mapping field) {
        this.field = field.toString();
    }

    /**
     * Sets the boost value that should be used.
     *
     * @param boost the boost value
     * @return the constraint itself for fluent method calls
     */
    public NotFilled withBoost(Float boost) {
        this.boost = boost;
        return this;
    }

    @Override
    public JSONObject toJSON() {
        return new BoolQueryBuilder().mustNot(new Filled(field).withBoost(boost)).toJSON();
    }
}
