/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.mongodb.BasicDBObject;
import org.bson.Document;
import sirius.db.DB;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.Mixing;
import sirius.db.mongo.constraints.MongoConstraint;
import sirius.db.mongo.constraints.MongoFilterFactory;
import sirius.kernel.async.ExecutionPoint;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Values;
import sirius.kernel.commons.Watch;
import sirius.kernel.di.std.Part;

import java.util.Objects;

/**
 * Base class for queries providing a filter builder.
 *
 * @param <S> the type of the subclass to fix the return types for abstract fluent method calls.
 */
public abstract class QueryBuilder<S> {

    protected Mongo mongo;
    protected BasicDBObject filterObject = new BasicDBObject();

    /**
     * Represents the filter factory used by mong queries.
     */
    public static final MongoFilterFactory FILTERS = new MongoFilterFactory();

    @Part
    protected static Mixing mixing;

    QueryBuilder(Mongo mongo) {
        this.mongo = mongo;
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
    @SuppressWarnings("unchecked")
    public S where(String key, Object value) {
        return where(FILTERS.eq(Mapping.named(key), value));
    }

    /**
     * Adds a complex filter which determines which documents should be selected.
     *
     * @param filter the filter to apply
     * @return the builder itself for fluent method calls
     */
    @SuppressWarnings("unchecked")
    public S where(MongoConstraint filter) {
        if (filterObject.containsField(filter.getKey()) && !Objects.equals(filterObject.get(filter.getKey()),
                                                                           filter.getObject())) {
            throw new IllegalArgumentException(Strings.apply("A constraint for %s was already specified. "
                                                             + "Please use Filter.and to combine multiple constraints "
                                                             + "on one field. Filter: %s",
                                                             filter.getKey(),
                                                             filterObject.toString()));
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

    /**
     * Returns an <tt>$explain</tt> for the filter represented by this query.
     *
     * @param collection the collection to execute the query for
     * @return the explanation delivered by MongoDB
     */
    public Doc explain(String collection) {
        return new Doc(mongo.db()
                            .getCollection(collection)
                            .find(filterObject)
                            .modifiers(new Document("$explain", true))
                            .first());
    }

    /**
     * Returns an <tt>$explain</tt> for the filter represented by this query.
     *
     * @param type the type of entities to generate the explanation for
     * @return the explanation delivered by MongoDB
     */
    public Doc explain(Class<?> type) {
        return explain(getRelationName(type));
    }

    protected void traceIfRequired(String collection, Watch w) {
        if (mongo.tracing && w.elapsedMillis() >= mongo.traceLimit) {
            String location = determineLocation();
            Doc explanation = explain(collection);
            mongo.traceData.put(location,
                                Tuple.create(collection + ": " + filterObject.toString() + " [" + w.duration() + "]",
                                             explanation.toString()));
        }

        if (w.elapsedMillis() > mongo.getLogQueryThresholdMillis()) {
            mongo.numSlowQueries.inc();
            DB.SLOW_DB_LOG.INFO("A slow MongoDB query was executed (%s): %s\n%s\n%s",
                                w.duration(),
                                collection,
                                filterObject,
                                ExecutionPoint.snapshot().toString());
        }
    }

    private String determineLocation() {
        // Tries to find the most useful (neither generic, nor reflection nor our framework) point in the stacktrace
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        int idx = 1;
        while (idx++ < stackTrace.length) {
            StackTraceElement currentElement = stackTrace[idx];
            if (!currentElement.getClassName().startsWith("sirius.db")
                && !currentElement.getClassName()
                                  .startsWith("com.sun")
                && !currentElement.getClassName().startsWith("java.")) {
                return currentElement.toString();
            }
        }

        // That's the best guess anyway
        return Values.of(stackTrace).at(5).asString("<unknown>");
    }

    protected static String getRelationName(Class<?> type) {
        return mixing.getDescriptor(type).getRelationName();
    }

    @Override
    public String toString() {
        return filterObject.toString();
    }
}
