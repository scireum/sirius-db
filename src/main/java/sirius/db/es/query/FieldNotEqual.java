/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.query;

import com.alibaba.fastjson.JSONObject;
import sirius.db.mixing.Mapping;

/**
 * Represents a constraint which checks if the given field has not the given value.
 */
public class FieldNotEqual extends FieldEqual {
    public FieldNotEqual(Mapping field, Object value) {
        this(field.toString(), value);
    }

    public FieldNotEqual(String field, Object value) {
        super(field, value);
    }

    @Override
    public JSONObject toJSON() {
        return new BoolQueryBuilder().mustNot(super.toJSON()).toJSON();
    }
}
