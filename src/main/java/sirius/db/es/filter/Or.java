/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.filter;

import com.alibaba.fastjson.JSONObject;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents a set of constraints of which at least once must be fulfilled.
 */
public class Or extends BaseFilter {

    private Filter[] filters;

    /**
     * Constructs a new constraint which requires one of the filters to be fulfilled.
     *
     * @param filters the filters to be fulfilled.
     */
    public Or(Filter... filters) {
        this.filters = filters;
    }

    @Override
    public JSONObject toJSON() {
        List<JSONObject> clauses =
                Arrays.stream(filters).map(Filter::toJSON).filter(Objects::nonNull).collect(Collectors.toList());
        if (clauses.isEmpty()) {
            return null;
        }

        return new JSONObject().fluentPut("bool", new JSONObject().fluentPut("should", clauses));
    }
}
