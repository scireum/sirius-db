/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Values;
import sirius.kernel.commons.Watch;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

/**
 * Base class for queries providing a filter builder.
 *
 * @param <S> the type of the subclass to fix the return types for abstract fluent method calls.
 */
public abstract class QueryBuilder<S> {

    protected Mongo mongo;
    protected BasicDBObject filterObject = new BasicDBObject();

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
    @SuppressWarnings("unchecked")
    public S where(String key, Object value) {
        return where(Filter.eq(key, value));
    }

    /**
     * Adds a complex filter which determines which documents should be selected.
     *
     * @param filter the filter to apply
     * @return the builder itself for fluent method calls
     */
    @SuppressWarnings("unchecked")
    public S where(Filter filter) {
        if (filterObject.containsField(filter.key)) {
            throw new IllegalArgumentException(Strings.apply("A constraint for %s was already specified. "
                                                             + "Please use Filter.and to combine multiple constraints "
                                                             + "on one field. Filter: %s",
                                                             filter.key,
                                                             filterObject.toString()));
        }
        filterObject.put(filter.key, filter.object);
        return (S) this;
    }

    protected void traceIfRequired(String collection, Watch w) {
        if (mongo.tracing && w.elapsedMillis() >= mongo.traceLimit) {
            String location = determineLocation();
            DBObject explanation = mongo.db().getCollection(collection).find(filterObject).explain();
            mongo.traceData.put(location,
                                Tuple.create(collection + ": " + filterObject.toString() + " [" + w.duration() + "]",
                                             explanation.toString()));
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

    /**
     * Maps Java 8 APIs to legacy objects used by Mongo DB.
     * <p>
     * Most notably these are the java.time classes.
     *
     * @param value the value to transform
     * @return the transformed value
     */
    public static Object transformValue(Object value) {
        if (value instanceof LocalDate) {
            return Date.from(((LocalDate) value).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
        }
        if (value instanceof LocalDateTime) {
            return Date.from(((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant());
        }
        if (value instanceof Instant) {
            return Date.from((Instant) value);
        }
        if (value != null && value.getClass().isEnum()) {
            return ((Enum<?>) value).name();
        }

        return value;
    }
}
