/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import org.bson.Document;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mongo.facets.MongoFacet;
import sirius.kernel.async.TaskContext;
import sirius.kernel.commons.Monoflop;
import sirius.kernel.commons.Value;
import sirius.kernel.commons.Watch;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Microtiming;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Fluent builder to build a find statement.
 */
public class Finder extends QueryBuilder<Finder> {

    private static final String KEY_MONGO = "mongo";
    private static final String OPERATOR_MATCH = "$match";
    private static final String OPERATOR_SAMPLE = "$sample";

    private Document fields;
    protected Document orderBy;
    private int skip;
    private int limit;
    private int batchSize;

    private ReadPreference readPreference;

    protected Finder(Mongo mongo, String database, @Nullable ReadPreference readPreference) {
        super(mongo, database);

        if (readPreference != null && readPreference != ReadPreference.primary()) {
            this.readPreference = readPreference;
        }
    }

    /**
     * Creates a copy of this finder which contains the same filters as this one.
     * <p>
     * Note that neither any limit for the selected fields are copied.
     *
     * @return a copy of this filter object which contains the same filters. Note that the filters are still a copy,
     * therefore modifying the filters of one object will not modify those of the other.
     */
    public Finder copyFilters() {
        Finder newFinder = new Finder(mongo, database, readPreference);
        transferFilters(newFinder);
        return newFinder;
    }

    /**
     * Limits the fields being returned to the given list.
     *
     * @param fieldsToReturn specified the list of fields to return
     * @return the builder itself for fluent method calls
     */
    public Finder selectFields(Mapping... fieldsToReturn) {
        fields = new Document();
        for (Mapping field : fieldsToReturn) {
            fields.put(field.toString(), 1);
        }

        return this;
    }

    /**
     * Limits the fields being returned to the given list.
     *
     * @param fieldsToReturn specified the list of fields to return
     * @return the builder itself for fluent method calls
     */
    public Finder selectFields(String... fieldsToReturn) {
        fields = new Document();
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
    public Finder orderByAsc(Mapping field) {
        return orderByAsc(field.toString());
    }

    /**
     * Adds a sort constraint to order by the given field ascending.
     *
     * @param field the field to order by.
     * @return the builder itself for fluent method calls
     */
    public Finder orderByAsc(String field) {
        if (orderBy == null) {
            orderBy = new Document();
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
    public Finder orderByDesc(Mapping field) {
        return orderByDesc(field.toString());
    }

    /**
     * Adds a sort constraint to order by the given field descending.
     *
     * @param field the field to order by.
     * @return the builder itself for fluent method calls
     */
    public Finder orderByDesc(String field) {
        if (orderBy == null) {
            orderBy = new Document();
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
     *
     * @param limit the max. number of items to return
     * @return the builder itself for fluent method calls
     */
    public Finder limit(int limit) {
        this.limit = limit;

        return this;
    }

    /**
     * Specifies the number of items to skip before items are added to the result.
     *
     * @param skip the number of items to skip. Values &lt;= 0 are ignored.
     * @return the query itself for fluent method calls
     */
    public Finder skip(int skip) {
        this.skip = skip;

        return this;
    }

    /**
     * Specifies the number of items per batch. This has no effect on the result. Low batchSizes can be used
     * to prevent cursor timeouts when using a time consuming processor, but will be slower because the cursor
     * makes more requests to the server.
     *
     * @param batchSize the number of items per batch. Values &lt;= 0 are ignored.
     * @return the query itself for fluent method calls
     */
    public Finder batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Executes the query for the given collection and returns a single document.
     *
     * @param type the type of entities to search
     * @return the founbd document wrapped as <tt>Optional</tt> or an empty one, if no document was found.
     */
    public Optional<Doc> singleIn(Class<?> type) {
        return singleIn(getRelationName(type));
    }

    /**
     * Executes the query for the given collection and returns a single document.
     *
     * @param collection the collection to search in
     * @return the founbd document wrapped as <tt>Optional</tt> or an empty one, if no document was found.
     */
    public Optional<Doc> singleIn(String collection) {
        Watch watch = Watch.start();
        try {
            FindIterable<Document> cur = buildCursor(collection);

            Document document = cur.first();

            if (document == null) {
                return Optional.empty();
            } else {
                return Optional.of(new Doc(document));
            }
        } finally {
            long callDuration = watch.elapsedMillis();
            mongo.callDuration.addValue(callDuration);
            if (readPreference != null && readPreference.isSecondaryOk()) {
                mongo.secondaryCallDuration.addValue(callDuration);
            }
            if (Microtiming.isEnabled()) {
                watch.submitMicroTiming(KEY_MONGO, "FIND ONE - " + collection + ": " + filterObject.keySet());
            }
            traceIfRequired(collection, watch);
        }
    }

    private FindIterable<Document> buildCursor(String collection) {
        FindIterable<Document> cursor = getMongoCollection(collection).find(filterObject);
        if (fields != null) {
            cursor.projection(fields);
        }
        if (orderBy != null) {
            cursor.sort(orderBy);
        }

        cursor.skip(skip);
        return cursor;
    }

    private MongoCollection<Document> getMongoCollection(String collection) {
        MongoCollection<Document> mongoCollection = mongo.db(database).getCollection(collection);
        if (readPreference != null) {
            mongoCollection = mongoCollection.withReadPreference(readPreference);
        }
        return mongoCollection;
    }

    /**
     * Executes the query for the given collection and calls the given processor for each document as long as it
     * returns <tt>true</tt>.
     *
     * @param type      the type of entities to search
     * @param processor the processor to handle matches, which also controls if further results should be processed
     */
    public void eachIn(Class<?> type, Predicate<Doc> processor) {
        eachIn(getRelationName(type), processor);
    }

    /**
     * Executes the query for the given collection and calls the given processor for each document as long as it
     * returns <tt>true</tt>.
     *
     * @param collection the collection to search in
     * @param processor  the processor to handle matches, which also controls if further results should be processed
     */
    public void eachIn(String collection, Predicate<Doc> processor) {
        if (Mongo.LOG.isFINE()) {
            Mongo.LOG.FINE("FIND: %s\nFilter: %s", collection, filterObject);
        }

        FindIterable<Document> cursor = buildCursor(collection);
        if (limit > 0) {
            cursor.limit(limit);
        }
        applyBatchSize(cursor);

        processCursor(cursor, processor, collection);
    }

    private void applyBatchSize(MongoIterable<Document> cursor) {
        if (batchSize > 0) {
            cursor.batchSize(batchSize);
        }
    }

    private void processCursor(MongoIterable<Document> cursor, Predicate<Doc> processor, String collection) {
        Watch watch = Watch.start();
        TaskContext taskContext = TaskContext.get();
        Monoflop shouldHandleTracing = Monoflop.create();

        for (Document doc : cursor) {
            if (shouldHandleTracing.firstCall()) {
                handleTracingAndReporting(collection, watch);
            }

            boolean keepGoing = processor.test(new Doc(doc));
            if (!keepGoing || (!taskContext.isActive() && longRunning)) {
                return;
            }
        }

        // If we didn't log any tracing data up until now, the result was completely empty and
        // we can (and should) safely log now - otherwise some entries might be missing in
        // the Microtiming...
        if (shouldHandleTracing.firstCall()) {
            handleTracingAndReporting(collection, watch);
        }
    }

    /**
     * Executes the query for the given collection in a random order and calls the given processor for each document as long as it
     * returns <tt>true</tt>.
     * <p>
     * Internally, this uses the <tt>$sample</tt> aggregation.
     *
     * @param collection the collection to search in
     * @param processor  the processor to handle matches, which also controls if further results should be processed
     */
    public void sample(String collection, Predicate<Doc> processor) {
        if (Mongo.LOG.isFINE()) {
            Mongo.LOG.FINE("SAMPLE: %s\nFilter: %s", collection, filterObject);
        }

        MongoIterable<Document> cursor =
                getMongoCollection(collection).aggregate(Arrays.asList(new BasicDBObject(OPERATOR_MATCH, filterObject),
                                                                       new BasicDBObject(OPERATOR_SAMPLE,
                                                                                         new BasicDBObject("size",
                                                                                                           limit))));

        applyBatchSize(cursor);
        processCursor(cursor, processor, collection);
    }

    private void handleTracingAndReporting(String collection, Watch watch) {
        long callDuration = watch.elapsedMillis();
        mongo.callDuration.addValue(callDuration);
        if (readPreference != null && readPreference.isSecondaryOk()) {
            mongo.secondaryCallDuration.addValue(callDuration);
        }
        if (Microtiming.isEnabled()) {
            watch.submitMicroTiming(KEY_MONGO, "FIND ALL - " + collection + ": " + filterObject.keySet());
        }
        traceIfRequired(collection, watch);
    }

    /**
     * Executes the query for the given collection and calls the given processor for each document.
     *
     * @param type      the type of entities to search
     * @param processor the processor to handle matches
     */
    public void allIn(Class<?> type, Consumer<Doc> processor) {
        allIn(getRelationName(type), processor);
    }

    /**
     * Executes the query for the given collection and calls the given processor for each document.
     *
     * @param collection the collection to search in
     * @param processor  the processor to handle matches
     */
    public void allIn(String collection, Consumer<Doc> processor) {
        eachIn(collection, doc -> {
            processor.accept(doc);
            return true;
        });
    }

    /**
     * Counts the number of documents in the result of the given query.
     * <p>
     * Note that limits are ignored for this query.
     *
     * @param type the type of entities to search
     * @return the number of documents found
     */
    public long countIn(Class<?> type) {
        return countIn(getRelationName(type));
    }

    /**
     * Counts the number of documents in the result of the given query.
     * <p>
     * Note that limits are ignored for this query.
     * If there are no filters in this query, an estimate is returned instead.
     *
     * @param collection the collection to search in
     * @return the number of documents found
     */
    public long countIn(String collection) {
        return countIn(collection, false, 0).orElse(0L);
    }

    /**
     * Counts the number of documents in the result of the given query.
     * <p>
     * Note that limits are ignored for this query.
     * If there are no filters in this query and forceAccurate is false, a pre-counted estimate is returned instead.
     *
     * @param collection    the collection to search in
     * @param forceAccurate if set to <tt>true</tt> we'll never use <b>estimatedDocumentCount</b> which is way more efficient but might return wrong values in case a cluster is active which had experienced an unclean shutdown.
     * @param maxTimeMS     the maximum process time for this cursor in milliseconds, 0 for unlimited
     * @return the number of documents found, wrapped in an Optional, or an empty Optional if the query timed out
     */
    public Optional<Long> countIn(String collection, boolean forceAccurate, long maxTimeMS) {
        Watch watch = Watch.start();
        try {
            if (filterObject.isEmpty() && !forceAccurate) {
                return Optional.of(getMongoCollection(collection).estimatedDocumentCount(new EstimatedDocumentCountOptions().maxTime(
                        maxTimeMS,
                        TimeUnit.MILLISECONDS)));
            }
            return Optional.of(getMongoCollection(collection).countDocuments(filterObject,
                                                                             new CountOptions().maxTime(maxTimeMS,
                                                                                                        TimeUnit.MILLISECONDS)));
        } catch (MongoExecutionTimeoutException e) {
            Exceptions.ignore(e);
            return Optional.empty();
        } finally {
            long callDuration = watch.elapsedMillis();
            mongo.callDuration.addValue(callDuration);
            if (readPreference != null && readPreference.isSecondaryOk()) {
                mongo.secondaryCallDuration.addValue(callDuration);
            }
            if (Microtiming.isEnabled()) {
                watch.submitMicroTiming(KEY_MONGO, "COUNT - " + collection + ": " + filterObject.keySet());
            }
            traceIfRequired(collection, watch);
        }
    }

    /**
     * Aggregates the documents in the result of the given query with an accumulator operator.
     * <p>
     * Note that limits are ignored for this query.
     *
     * @param type     the type of entities to aggregate
     * @param field    the field to aggregate
     * @param operator the accumulation operator to aggregate with
     * @return the result of the accumulation (usually Integer, Double or List)
     * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/group/#accumulator-operator">MongoDB Reference</a>
     */
    public Value aggregateIn(@Nonnull Class<?> type, @Nonnull Mapping field, @Nonnull String operator) {
        return aggregateIn(getRelationName(type), field, operator);
    }

    /**
     * Aggregates the documents in the result of the given query with an accumulator operator.
     * <p>
     * Note that limits are ignored for this query.
     *
     * @param collection the collection to search in
     * @param field      the field to aggregate
     * @param operator   the accumulation operator to aggregate with
     * @return the result of the accumulation (usually Integer, Double or List)
     * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/group/#accumulator-operator">MongoDB Reference</a>
     */
    public Value aggregateIn(@Nonnull String collection, @Nonnull Mapping field, @Nonnull String operator) {
        Watch watch = Watch.start();
        BasicDBObject groupStage = new BasicDBObject().append(Mango.ID_FIELD, null)
                                                      .append("result", new BasicDBObject(operator, "$" + field));
        try (MongoCursor<Document> queryResult = getMongoCollection(collection).aggregate(Arrays.asList(new BasicDBObject(
                OPERATOR_MATCH,
                filterObject), new BasicDBObject("$group", groupStage))).iterator()) {
            if (queryResult.hasNext()) {
                return Value.of(queryResult.next().get("result"));
            } else {
                return Value.EMPTY;
            }
        } finally {
            long callDuration = watch.elapsedMillis();
            mongo.callDuration.addValue(callDuration);
            if (readPreference != null && readPreference.isSecondaryOk()) {
                mongo.secondaryCallDuration.addValue(callDuration);
            }
            if (Microtiming.isEnabled()) {
                watch.submitMicroTiming(KEY_MONGO,
                                        "AGGREGATE - "
                                        + collection
                                        + "."
                                        + field
                                        + " ("
                                        + operator
                                        + "): "
                                        + filterObject.keySet());
            }
            traceIfRequired("aggregate-" + collection, watch);
        }
    }

    /**
     * Executes the given list of facets using the filters specified for this query.
     *
     * @param descriptor the entity descriptor to query
     * @param facets     the facets to execute
     */
    public void executeFacets(@Nonnull EntityDescriptor descriptor, @Nullable List<MongoFacet> facets) {
        if (facets == null || facets.isEmpty()) {
            return;
        }

        Watch watch = Watch.start();
        String collection = descriptor.getRelationName();
        BasicDBObject facetStage = new BasicDBObject();
        for (MongoFacet facet : facets) {
            facet.emitFacets(descriptor, facetStage::append);
        }

        try (MongoCursor<Document> queryResult = getMongoCollection(collection).aggregate(Arrays.asList(new BasicDBObject(
                OPERATOR_MATCH,
                filterObject), new BasicDBObject("$facet", facetStage))).iterator()) {

            if (queryResult.hasNext()) {
                Doc doc = new Doc(queryResult.next());
                for (MongoFacet facet : facets) {
                    facet.digest(doc);
                }
            }
        } finally {
            long callDuration = watch.elapsedMillis();
            mongo.callDuration.addValue(callDuration);
            if (readPreference != null && readPreference.isSecondaryOk()) {
                mongo.secondaryCallDuration.addValue(callDuration);
            }
            if (Microtiming.isEnabled()) {
                watch.submitMicroTiming(KEY_MONGO, "FACETS - " + collection + ": " + filterObject.keySet());
            }
            traceIfRequired("facets-" + collection, watch);
        }
    }
}
