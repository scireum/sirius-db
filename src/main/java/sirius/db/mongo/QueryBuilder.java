/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import sirius.db.DB;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.Mixing;
import sirius.db.mongo.constraints.MongoConstraint;
import sirius.db.mongo.constraints.MongoFilterFactory;
import sirius.kernel.async.ExecutionPoint;
import sirius.kernel.commons.Watch;
import sirius.kernel.di.std.Part;

import java.util.List;
import java.util.Objects;

/**
 * Base class for queries providing a filter builder.
 *
 * @param <S> the type of the subclass to fix the return types for abstract fluent method calls.
 */
public abstract class QueryBuilder<S> {

    protected final String database;
    protected final Mongo mongo;
    protected BasicDBObject filterObject = new BasicDBObject();

    /**
     * Represents the filter factory used by mong queries.
     */
    public static final MongoFilterFactory FILTERS = new MongoFilterFactory();

    @Part
    protected static Mixing mixing;

    QueryBuilder(Mongo mongo, String database) {
        this.mongo = mongo;
        this.database = database;
    }

    /**
     * Adds a condition which determines which documents should be selected.
     *
     * @param key   the name of the field to filter on
     * @param value the value to filter on
     * @return the builder itself for fluent method calls
     */
    public S where(Mapping key, Object value) {
        return where(key.toString(), value);
    }

    /**
     * Adds a condition which determines which documents should be selected.
     *
     * @param key   the name of the field to filter on
     * @param value the value to filter on
     * @return the builder itself for fluent method calls
     */
    public S where(String key, Object value) {
        return where(FILTERS.eq(Mapping.named(key), value));
    }

    /**
     * Adds an equals constraint to the query is the given condition is fulfilled (<tt>true</tt>).
     *
     * @param field     the name of the field to filter on
     * @param value     the value to filter on
     * @param condition the condition which must be <tt>true</tt> in order to create the constraint
     * @return the builder itself for fluent method calls
     */
    @SuppressWarnings("unchecked")
    public S whereIf(Mapping field, Object value, boolean condition) {
        if (condition) {
            where(field, value);
        }

        return (S) this;
    }

    /**
     * Adds an equals constraint to the query unless the given value is <tt>null</tt>.
     *
     * @param field the name of the field to filter on
     * @param value the value to filter on
     * @return the builder itself for fluent method calls
     */
    public S whereIgnoreNull(Mapping field, Object value) {
        return whereIf(field, value, value != null);
    }

    /**
     * Adds a complex filter which determines which documents should be selected.
     *
     * @param filter the filter to apply
     * @return the builder itself for fluent method calls
     */
    @SuppressWarnings("unchecked")
    public S where(MongoConstraint filter) {
        if (filterObject.containsField(filter.getKey())) {
            Object other = filterObject.get(filter.getKey());
            if ("$and".equals(filter.getKey())) {
                ((List<MongoConstraint>) other).addAll((List<MongoConstraint>) filter.getObject());
                return (S) this;
            }

            if (Objects.equals(other, filter.getObject())) {
                return (S) this;
            }

            filterObject.remove(filter.getKey());
            return where(new MongoConstraint("$and",
                                             Lists.newArrayList(new BasicDBObject(filter.getKey(), other),
                                                                new BasicDBObject(filter.getKey(),
                                                                                  filter.getObject()))));
        }

        filterObject.put(filter.getKey(), filter.getObject());
        return (S) this;
    }

    /**
     * Applies all filters of this query to the given target.
     *
     * @param target the target to be supplied with the filters of this query
     */
    public void transferFilters(QueryBuilder<?> target) {
        target.filterObject.clear();
        target.filterObject.putAll(filterObject.toMap());
    }

    protected void traceIfRequired(String collection, Watch w) {
        if (w.elapsedMillis() > mongo.getLogQueryThresholdMillis()) {
            mongo.numSlowQueries.inc();
            DB.SLOW_DB_LOG.INFO("A slow MongoDB query was executed (%s): %s\n%s\n%s",
                                w.duration(),
                                collection,
                                filterObject,
                                ExecutionPoint.snapshot().toString());
        }
    }

    protected static String getRelationName(Class<?> type) {
        return mixing.getDescriptor(type).getRelationName();
    }

    @Override
    public String toString() {
        return filterObject.toString();
    }
}
