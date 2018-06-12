/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.filter;

import com.alibaba.fastjson.JSONObject;

/**
 * Negates the given constraint.
 */
public class Not extends BaseFilter {

    private Filter inner;

    /**
     * Inverts the given inner filter.
     *
     * @param inner the inner filter to invert
     */
    public Not(Filter inner) {
        this.inner = inner;
    }

    @Override
    public JSONObject toJSON() {
        return new BoolQueryBuilder().mustNot(inner.toJSON()).toJSON();
    }
}
