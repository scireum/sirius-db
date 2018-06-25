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
import sirius.kernel.commons.Strings;

/**
 * Represents a constraint which checks if the given field starts with the given value.
 * <p>
 * To prevent funny OutOfMemoryErrors the maximal number of tokens being expanded is 256.
 */
public class Prefix extends BaseFilter {
    private final String field;
    private String value;
    private Float boost;

    /**
     * Createsa prefix filter for the given field and value.
     *
     * @param field the field to filter on
     * @param value the prefix to filter by
     */
    public Prefix(String field, String value) {
        this.field = field;
        this.value = value;
    }

    /**
     * Createsa prefix filter for the given field and value.
     *
     * @param field the field to filter on
     * @param value the prefix to filter by
     */
    public Prefix(Mapping field, String value) {
        this.field = field.toString();
        this.value = value;
    }

    /**
     * Sets the boost value that should be used for matching terms.
     *
     * @param boost the boost value
     * @return the constraint itself for fluent method calls
     */
    public Prefix withBoost(Float boost) {
        this.boost = boost;
        return this;
    }

    @Override
    public JSONObject toJSON() {
        if (Strings.isEmpty(value)) {
            return null;
        }

        JSONObject innerQuery = new JSONObject().fluentPut(field,
                                                           new JSONObject().fluentPut("value", this.value)
                                                                           .fluentPut("rewrite", "top_terms_256"));
        if (boost != null) {
            innerQuery = innerQuery.fluentPut("boost", boost);
        }

        return new JSONObject().fluentPut("prefix", innerQuery);
    }
}
