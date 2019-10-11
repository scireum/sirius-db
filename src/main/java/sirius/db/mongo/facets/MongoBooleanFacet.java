/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.facets;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.Document;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mongo.Doc;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Represents a facet which aggregates a given boolean field.
 * <p>
 * This will generate a $sortByCount for the given field.
 */
public class MongoBooleanFacet extends MongoFacet {

    private final Mapping field;
    private int numTrue;
    private int numFalse;
    private Consumer<MongoBooleanFacet> completionCallback;

    /**
     * Generates a facet with the given name, for the given field.
     *
     * @param name  the name of the facet
     * @param field the field to aggregate on
     */
    public MongoBooleanFacet(String name, Mapping field) {
        super(name);
        this.field = field;
    }

    /**
     * Creates a term facet for the given field.
     *
     * @param field the field to aggregate on
     */
    public MongoBooleanFacet(Mapping field) {
        this(field.toString(), field);
    }

    /**
     * Specifies the callback to invoke once the facet was been computed completely.
     *
     * @param completionCallback the callback to invoke
     * @return the facet itself for fluent method calls
     */
    public MongoBooleanFacet onComplete(Consumer<MongoBooleanFacet> completionCallback) {
        this.completionCallback = completionCallback;
        return this;
    }

    @Override
    public void emitFacets(EntityDescriptor descriptor, BiConsumer<String, DBObject> facetConsumer) {
        BasicDBList facet = new BasicDBList();
        String fieldName = descriptor.findProperty(field.toString()).getPropertyName();
        facet.add(new BasicDBObject().append("$sortByCount", "$" + fieldName));

        facetConsumer.accept(name, facet);
    }

    @Override
    public void digest(Doc result) {
        List<Object> results = result.getList(name);
        for (Object resultItem : results) {
            Document resultDoc = (Document) resultItem;
            int count = resultDoc.getInteger("count", 0);
            if (resultDoc.getBoolean("_id", false)) {
                numTrue = count;
            } else {
                numFalse = count;
            }
        }

        if (completionCallback != null) {
            completionCallback.accept(this);
        }
    }

    /**
     * Returns the number of documents which have a <tt>true</tt> in the given field.
     *
     * @return the number of document containing <tt>true</tt>
     */
    public int getNumTrue() {
        return numTrue;
    }

    /**
     * Returns the number of documents which have a <tt>false</tt> in the given field.
     *
     * @return the number of document containing <tt>false</tt>
     */
    public int getNumFalse() {
        return numFalse;
    }
}
