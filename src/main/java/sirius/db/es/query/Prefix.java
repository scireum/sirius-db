/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.query;

import com.alibaba.fastjson.JSONObject;
import sirius.kernel.commons.Strings;

/**
 * Represents a constraint which checks if the given field starts with the given value.
 * <p>
 * To prevent funny OutOfMemoryErrors the number of tokens being expanded is 256.
 */
public class Prefix extends BaseFilter {
    private final String field;
    private String value;
    private Float boost;

    /*
     * Use the #on(String, Object) factory method
     */
    public Prefix(String field, String value) {
        this.field = field;
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
        if (Strings.isFilled(value)) {
            if (boost == null) {
                return new JSONObject().fluentPut("prefix",
                                                  new JSONObject().fluentPut(field, value)
                                                                  .fluentPut("rewrite", "top_terms_256"));
            } else {
                return new JSONObject().fluentPut("prefix",
                                                  new JSONObject().fluentPut(field,
                                                                             new JSONObject().fluentPut("value", value)
                                                                                             .fluentPut("boost", boost))
                                                                  .fluentPut("rewrite", "top_terms_256"));
            }
        }

        return null;
    }
}
