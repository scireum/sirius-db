/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

/**
 * Represents a filter constraint to build complex filters.
 */
public class Filter {

    protected String key;
    protected Object object;

    private Filter() {
    }

    /**
     * Creates a new filter with the given key and filter object.
     *
     * @param key    the key used in the filter object
     * @param object the filter expression as object
     */
    public Filter(String key, Object object) {
        this.key = key;
        this.object = object;
    }

    private static Filter relOp(String operator, Filter[] filters) {
        Filter filter = new Filter();
        filter.key = operator;
        BasicDBList clauses = new BasicDBList();
        for (Filter subFilter : filters) {
            clauses.add(new BasicDBObject(subFilter.key, subFilter.object));
        }
        filter.object = clauses;

        return filter;
    }

    private static Filter op(String operator, String key, Object value) {
        Filter filter = new Filter();
        filter.key = key;
        filter.object = new BasicDBObject(operator, QueryBuilder.transformValue(value));

        return filter;
    }

    /**
     * Builds an AND filter with the given filters as clauses.
     *
     * @param filters the clauses of the AND filter
     * @return a new filter representing the AND combination of the given clauses
     */
    public static Filter and(Filter... filters) {
        return relOp("$and", filters);
    }

    /**
     * Builds an OR filter with the given filters as clauses.
     *
     * @param filters the clauses of the OR filter
     * @return a new filter representing the OR combination of the given clauses
     */
    public static Filter or(Filter... filters) {
        return relOp("$or", filters);
    }

    /**
     * Builds a filter which verifies that the given field exists.
     *
     * @param key the name of the field to check
     * @return a filter representing the given operation
     */
    public static Filter exists(String key) {
        return op("$exists", key, true);
    }

    /**
     * Builds a filter which verifies that the given field does not exist.
     *
     * @param key the name of the field to check
     * @return a filter representing the given operation
     */
    public static Filter notExists(String key) {
        return op("$exists", key, false);
    }

    /**
     * Builds a filter which represents <tt>field == value</tt>
     *
     * @param key   the name of the field to check
     * @param value the value to compare against
     * @return a filter representing the given operation
     */
    public static Filter eq(String key, Object value) {
        return op("$eq", key, value);
    }

    /**
     * Builds a filter which represents a regex filter for the given field and expression.
     *
     * @param key        the name of the field to check
     * @param expression the regular expression to apply
     * @param options    the options to apply like "i" to match case insensitive
     * @return a filter representing the given operation
     */
    public static Filter regex(String key, Object expression, String options) {
        Filter filter = new Filter();
        filter.key = key;
        filter.object = new BasicDBObject("$regex", expression).append("$options", options);

        return filter;
    }

    /**
     * Builds a filter which represents a geospatial query.
     *
     * @param key               the name of the field to check
     * @param geometry          the geometry used to filter by
     * @param maxDistanceMeters the max distance to consider relevant
     * @return a filter representing the given operation
     */
    public static Filter nearSphere(String key, BasicDBObject geometry, int maxDistanceMeters) {
        Filter filter = new Filter();
        filter.key = key;
        filter.object = new BasicDBObject("$nearSphere",
                                          new BasicDBObject().append("$geometry", geometry)
                                                             .append("$maxDistance", maxDistanceMeters));

        return filter;
    }

    /**
     * Builds a filter which represents a geospatial query for a point.
     *
     * @param key               the name of the field to check
     * @param lat               the latitude of the point used as search geometry.
     * @param lon               the longitude of the point used as search geometry.
     * @param maxDistanceMeters the max distance to consider relevant
     * @return a filter representing the given operation
     */
    public static Filter nearSphere(String key, double lat, double lon, int maxDistanceMeters) {
        BasicDBList coordinates = new BasicDBList();
        coordinates.add(lat);
        coordinates.add(lon);

        return nearSphere(key,
                          new BasicDBObject().append("type", "Point").append("coordinates", coordinates),
                          maxDistanceMeters);
    }

    /**
     * Builds a filter which represents <tt>field != value</tt>
     *
     * @param key   the name of the field to check
     * @param value the value to compare against
     * @return a filter representing the given operation
     */
    public static Filter ne(String key, Object value) {
        return op("$ne", key, value);
    }

    /**
     * Builds a filter which represents <tt>field &gt; value</tt>
     *
     * @param key   the name of the field to check
     * @param value the value to compare against
     * @return a filter representing the given operation
     */
    public static Filter gt(String key, Object value) {
        return op("$gt", key, value);
    }

    /**
     * Builds a filter which represents <tt>field &lt; value</tt>
     *
     * @param key   the name of the field to check
     * @param value the value to compare against
     * @return a filter representing the given operation
     */
    public static Filter lt(String key, Object value) {
        return op("$lt", key, value);
    }

    /**
     * Builds a filter which represents <tt>field &gt;= value</tt>
     *
     * @param key   the name of the field to check
     * @param value the value to compare against
     * @return a filter representing the given operation
     */
    public static Filter gte(String key, Object value) {
        return op("$gte", key, value);
    }

    /**
     * Builds a filter which represents <tt>field &lt;= value</tt>
     *
     * @param key   the name of the field to check
     * @param value the value to compare against
     * @return a filter representing the given operation
     */
    public static Filter lte(String key, Object value) {
        return op("$lte", key, value);
    }

    /**
     * Builds a filter which determines if the given field equals one of the given values.
     *
     * @param key    the name of the field to check
     * @param values the values to compare against
     * @return a filter representing the given operation
     */
    public static Filter in(String key, Object... values) {
        BasicDBList list = new BasicDBList();
        for (Object value : values) {
            list.add(QueryBuilder.transformValue(value));
        }
        return op("$in", key, list);
    }

    /**
     * Builds a filter which verifies that the given field equals none of the given values.
     *
     * @param key    the name of the field to check
     * @param values the values to compare against
     * @return a filter representing the given operation
     */
    public static Filter nin(String key, Object... values) {
        BasicDBList list = new BasicDBList();
        for (Object value : values) {
            list.add(QueryBuilder.transformValue(value));
        }
        return op("$nin", key, list);
    }
}
