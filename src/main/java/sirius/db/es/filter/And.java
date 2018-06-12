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
 * Represents a set of constraints of which every one must be fulfilled.
 */
public class And extends BaseFilter {

    private Filter[] filters;

    /**
     * Constructs a new constraint which requires all filters to be fulfilled.
     *
     * @param filters the filters to be fulfilled.
     */
    public And(Filter... filters) {
        this.filters = filters;
    }

    @Override
    public JSONObject toJSON() {
        List<JSONObject> clauses =
                Arrays.stream(filters).map(Filter::toJSON).filter(Objects::nonNull).collect(Collectors.toList());
        if (clauses.isEmpty()) {
            return null;
        }

        if (clauses.size() == 1) {
            return clauses.get(0);
        }

        return new JSONObject().fluentPut("bool", new JSONObject().fluentPut("must", clauses));
    }
}
