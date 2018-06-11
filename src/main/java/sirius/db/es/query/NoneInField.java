/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.query;

import com.alibaba.fastjson.JSONObject;

import java.util.Collection;

/**
 * Represents a constraint which verifies that the given field contains none of the given values.
 */
public class NoneInField extends BaseFilter {

    private final Collection<?> values;
    private final String field;

    /*
     * Use the #on(List, String) factory method
     */
    public NoneInField(Collection<?> values, String field) {
        this.values = values;
        this.field = field;
    }

    @Override
    public JSONObject toJSON() {
        if (values == null || values.isEmpty()) {
            return null;
        }

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        for (Object value : values) {
            boolQueryBuilder.mustNot(new FieldEqual(field, FieldEqual.transformFilterValue(value)));
        }

        return boolQueryBuilder.toJSON();
    }
}
