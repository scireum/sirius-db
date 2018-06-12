/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.filter;

import com.alibaba.fastjson.JSONObject;
import sirius.db.es.ElasticQuery;

/**
 * Represents a filter constraint to be used in a {@link ElasticQuery}.
 */
public interface Filter {

    /**
     * Constructs a JSON object representing the filter.
     *
     * @return a JSON representation of the filter / constraint
     */
    JSONObject toJSON();
}
