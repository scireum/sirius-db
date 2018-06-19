/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.filter;

import com.alibaba.fastjson.JSONObject;
import sirius.db.es.properties.ESStringMapProperty;
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
public class NestedQuery extends BaseFilter {

    private String path;
    private List<Filter> innerQueries = new ArrayList<>();

    /**
     * Creates a new nested query for elements in the given field / list.
     *
     * @param path the name of the list to search in.
     */
    public NestedQuery(Mapping path) {
        this.path = path.toString();
    }

    /**
     * Creates a new nested query for elements in the given field / list.
     *
     * @param path the name of the list to search in.
     */
    public NestedQuery(String path) {
        this.path = path;
    }

    /**
     * Creates a filter which ensures, that a nested {@link sirius.db.mixing.types.StringMap}
     * or {@link sirius.db.mixing.types.StringListMap} contains the given key and value.
     *
     * @param mapField the property which contains either a <tt>StringMap</tt> or <tt>StringListMap</tt>
     * @param key      the key to check for
     * @param value    the value to check for
     * @return the filter
     */
    public static NestedQuery mapContains(Mapping mapField, String key, String value) {
        return new NestedQuery(mapField).eq(mapField.nested(Mapping.named(ESStringMapProperty.KEY)), key)
                                        .eq(mapField.nested(Mapping.named(ESStringMapProperty.VALUE)), value);
    }

    /**
     * Appends an inner filter.
     * <p>
     * Note that all fields must be fully qualified (e.g. list.valueX instead of simly valueX).
     *
     * @param filter the filter to add
     * @return the nested query itself for fluent method calls
     */
    public NestedQuery withQuery(Filter filter) {
        this.innerQueries.add(filter);
        return this;
    }

    /**
     * Adds a simple {@link FieldEqual} filter as inner filter.
     *
     * @param field the fully qualified field to filter on
     * @param value the value to filter on
     * @return the nested query itself for fluent method calls
     */
    public NestedQuery eq(String field, Object value) {
        this.innerQueries.add(new FieldEqual(field, value));
        return this;
    }

    /**
     * Adds a simple {@link FieldEqual} filter as inner filter.
     *
     * @param field the fully qualified field to filter on
     * @param value the value to filter on
     * @return the nested query itself for fluent method calls
     */
    public NestedQuery eq(Mapping field, Object value) {
        this.innerQueries.add(new FieldEqual(field, value));
        return this;
    }

    @Override
    public JSONObject toJSON() {
        BoolQueryBuilder builder = new BoolQueryBuilder();
        innerQueries.forEach(builder::must);

        JSONObject query = builder.toJSON();
        if (query == null) {
            return null;
        }

        JSONObject result = new JSONObject();
        result.put("nested", new JSONObject().fluentPut("path", path).fluentPut("query", query));

        return result;
    }
}
