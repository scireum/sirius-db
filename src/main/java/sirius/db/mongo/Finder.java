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

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Fluent builder to build a find statement.
 */
public class Finder {

    private Mongo mongo;
    private BasicDBObject filterObject = new BasicDBObject();
    private BasicDBObject fields;

    protected Finder(Mongo mongo) {
        this.mongo = mongo;
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
     * Adds a condition which determines which documents should be found.
     *
     * @param key   the name of the field to filter on
     * @param value the value to filter on
     * @return the builder itself for fluent method calls
     */
    public Finder where(String key, Object value) {
        filterObject.put(key, value);

        return this;
    }

    /**
     * Executes the query for the given collection and returns a single document.
     *
     * @param collection the collection to search in
     * @return the founbd document wrapped as <tt>Optional</tt> or an empty one, if no document was found.
     */
    public Optional<Document> singleIn(String collection) {
        DBObject obj = mongo.db().getCollection(collection).findOne(filterObject, fields);
        if (obj == null) {
            return Optional.empty();
        } else {
            return Optional.of(new Document(obj));
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
        DBCursor cur = mongo.db().getCollection(collection).find(filterObject, fields);
        TaskContext ctx = TaskContext.get();
        while (cur.hasNext() && ctx.isActive()) {
            boolean keepGoing = processor.apply(new Document(cur.next()));
            if (!keepGoing) {
                break;
            }
        }
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
}
