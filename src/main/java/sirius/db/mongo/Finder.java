/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import sirius.kernel.async.TaskContext;
import sirius.kernel.commons.Watch;
import sirius.kernel.health.Microtiming;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Fluent builder to build a find statement.
 */
public class Finder extends QueryBuilder<Finder> {

    private static final String KEY_MONGO = "mongo";
    private BasicDBObject fields;
    private BasicDBObject orderBy;
    private int skip;
    private int limit;

    protected Finder(Mongo mongo) {
        super(mongo);
    }

    /**
     * Limits the fields being returned to the given list.
     *
     * @param fieldsToReturn specified the list of fields to return
     * @return the builder itself for fluent method calls
     */
    public Finder selectFields(String... fieldsToReturn) {
        fields = new BasicDBObject();
        for (String field : fieldsToReturn) {
            fields.put(field, 1);
        }

        return this;
    }

    /**
     * Adds a sort constraint to order by the given field ascending.
     *
     * @param field the field to order by.
     * @return the builder itself for fluent method calls
     */
    public Finder orderByAsc(String field) {
        if (orderBy == null) {
            orderBy = new BasicDBObject();
        }
        orderBy.put(field, 1);

        return this;
    }

    /**
     * Adds a sort constraint to order by the given field descending.
     *
     * @param field the field to order by.
     * @return the builder itself for fluent method calls
     */
    public Finder orderByDesc(String field) {
        if (orderBy == null) {
            orderBy = new BasicDBObject();
        }
        orderBy.put(field, -1);

        return this;
    }

    /**
     * Adds a limit to the query.
     *
     * @param skip  the number of items to skip (used for pagination).
     * @param limit the max. number of items to return (exluding those who have been skipped).
     * @return the builder itself for fluent method calls
     */
    public Finder limit(int skip, int limit) {
        this.skip = skip;
        this.limit = limit;

        return this;
    }

    /**
     * Adds a limit to the query.
     * <p>
     * This is boilerplate for {@code finder.limit(0, limit)}.
     *
     * @param limit the max. number of items to return
     * @return the builder itself for fluent method calls
     */
    public Finder limit(int limit) {
        this.skip = 0;
        this.limit = limit;

        return this;
    }

    /**
     * Executes the query for the given collection and returns a single document.
     *
     * @param collection the collection to search in
     * @return the founbd document wrapped as <tt>Optional</tt> or an empty one, if no document was found.
     */
    public Optional<Document> singleIn(String collection) {
        Watch w = Watch.start();
        try {
            DBObject obj = mongo.db().getCollection(collection).findOne(filterObject, fields, orderBy);
            if (obj == null) {
                return Optional.empty();
            } else {
                return Optional.of(new Document(obj));
            }
        } finally {
            mongo.callDuration.addValue(w.elapsedMillis());
            if (Microtiming.isEnabled()) {
                w.submitMicroTiming(KEY_MONGO, "FIND ONE - " + collection + ": " + filterObject);
            }
            traceIfRequired(collection, w);
        }
    }

    /**
     * Executes the query for the given collection and calls the given processor for each document as long as it
     * returns <tt>true</tt>.
     *
     * @param collection the collection to search in
     * @param processor  the processor to handle matches, which also controls if further results should be processed
     */
    public void eachIn(String collection, Function<Document, Boolean> processor) {
        Watch w = Watch.start();

        DBCursor cur = mongo.db().getCollection(collection).find(filterObject, fields);
        if (orderBy != null) {
            cur.sort(orderBy);
        }
        cur.skip(skip);
        if (limit > 0) {
            cur.limit(limit);
        }
        handleTracingAndReporting(collection, w, cur);

        TaskContext ctx = TaskContext.get();
        while (cur.hasNext() && ctx.isActive()) {
            boolean keepGoing = processor.apply(new Document(cur.next()));
            if (!keepGoing) {
                break;
            }
        }
    }

    @SuppressWarnings("squid:S899")
    private void handleTracingAndReporting(String collection, Watch w, DBCursor cur) {
        cur.hasNext();
        mongo.callDuration.addValue(w.elapsedMillis());
        if (Microtiming.isEnabled()) {
            w.submitMicroTiming(KEY_MONGO, "FIND ALL - " + collection + ": " + filterObject);
        }
        traceIfRequired(collection, w);
    }

    /**
     * Executes the query for the given collection and calls the given processor for each document.
     *
     * @param collection the collection to search in
     * @param processor  the processor to handle matches
     */
    public void allIn(String collection, Consumer<Document> processor) {
        eachIn(collection, d -> {
            processor.accept(d);
            return true;
        });
    }

    /**
     * Counts the number of documents in the result of the given query.
     * <p>
     * Note that limits are ignored for this query.
     *
     * @param collection the collection to search in
     * @return the number of documents found
     */
    public long countIn(String collection) {
        Watch w = Watch.start();
        try {
            DBCursor cur = mongo.db().getCollection(collection).find(filterObject, fields);
            return cur.count();
        } finally {
            mongo.callDuration.addValue(w.elapsedMillis());
            if (Microtiming.isEnabled()) {
                w.submitMicroTiming(KEY_MONGO, "COUNT - " + collection + ": " + filterObject);
            }
            traceIfRequired(collection, w);
        }
    }
}
