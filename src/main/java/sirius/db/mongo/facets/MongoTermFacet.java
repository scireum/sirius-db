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
import sirius.kernel.commons.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Represents a term facet which aggregates a given field (counts individual values).
 * <p>
 * This will generate a $sortByCount for the given field.
 */
public class MongoTermFacet extends MongoFacet {

    private final Mapping field;
    private List<Tuple<String, Integer>> values;
    private Consumer<MongoTermFacet> completionCallback;

    /**
     * Generates a facet with the given name, for the given field.
     *
     * @param name  the name of the facet
     * @param field the field to aggregate on
     */
    public MongoTermFacet(String name, Mapping field) {
        super(name);
        this.field = field;
    }

    /**
     * Creates a term facet for the given field.
     *
     * @param field the field to aggregate on
     */
    public MongoTermFacet(Mapping field) {
        this(field.toString(), field);
    }

    /**
     * Specifies the callback to invoke once the facet was been computed completely.
     *
     * @param completionCallback the callback to invoke
     * @return the facet itself for fluent method calls
     */
    public MongoTermFacet onComplete(Consumer<MongoTermFacet> completionCallback) {
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
        this.values = new ArrayList<>();

        List<Object> results = result.getList(name);
        for (Object resultItem : results) {
            Document resultDoc = (Document) resultItem;
            String term = resultDoc.getString("_id");
            int count = resultDoc.getInteger("count", 0);
            values.add(Tuple.create(term, count));
        }

        if (completionCallback != null) {
            completionCallback.accept(this);
        }
    }

    /**
     * Returns the list of filter values.
     *
     * @return a list of names (terms) and their number of matches
     */
    public List<Tuple<String, Integer>> getValues() {
        return Collections.unmodifiableList(values);
    }
}
