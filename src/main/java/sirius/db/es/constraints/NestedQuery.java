/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.constraints;

import com.alibaba.fastjson.JSONObject;
import sirius.db.es.Elastic;
import sirius.db.mixing.Mapping;

import java.util.ArrayList;
import java.util.List;

/**
 * Executes a <tt>nested</tt> query.
 * <p>
 * Nested queries must be used along with {@link sirius.db.mixing.types.NestedList nested lists}. Otherwise,
 * if there are several objects nested in a list, an entity would also match, if the filtered properties match
 * against any of the nested objects instead of all in a single one - which is most commonly wanted.
 * <p>
 * See: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-nested-query.html
 */
public class NestedQuery {

    private final Mapping path;
    private final List<ElasticConstraint> innerQueries = new ArrayList<>();

    /**
     * Creates a new nested query for elements in the given field / list.
     *
     * @param path the name of the list to search in.
     */
    protected NestedQuery(Mapping path) {
        this.path = path;
    }

    /**
     * Appends an inner constraint.
     * <p>
     * Note that all fields must be fully qualified (e.g. list.valueX instead of simply valueX).
     *
     * @param constraint the filter to add
     * @return the nested query itself for fluent method calls
     */
    public NestedQuery where(ElasticConstraint constraint) {
        if (constraint != null) {
            this.innerQueries.add(constraint);
        }
        return this;
    }

    /**
     * Adds a simple {@link ElasticFilterFactory#eq(Mapping, Object)} filter as inner filter.
     *
     * @param field the fully qualified field to filter on
     * @param value the value to filter on
     * @return the nested query itself for fluent method calls
     */
    public NestedQuery eq(Mapping field, Object value) {
        return where(Elastic.FILTERS.eq(field, value));
    }

    /**
     * Compiles the query into a constraint.
     *
     * @return the effective constraint which can be applied on a query
     */
    public ElasticConstraint build() {
        BoolQueryBuilder builder = new BoolQueryBuilder();
        innerQueries.forEach(builder::must);

        JSONObject query = builder.build();
        if (query == null) {
            return null;
        }

        JSONObject result = new JSONObject();
        result.put("nested", new JSONObject().fluentPut("path", path.toString()).fluentPut("query", query));

        return new ElasticConstraint(result);
    }
}
