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
import sirius.kernel.commons.Amount;
import sirius.kernel.commons.Value;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Represents a term facet which computes the avarage of a given field.
 */
public class MongoAvgFacet extends MongoFacet {

    private final Mapping field;
    private Amount value = Amount.NOTHING;
    private Consumer<MongoAvgFacet> completionCallback;

    /**
     * Generates a facet with the given name, for the given field.
     *
     * @param name  the name of the facet
     * @param field the field to aggregate on
     */
    public MongoAvgFacet(String name, Mapping field) {
        super(name);
        this.field = field;
    }

    /**
     * Creates a term facet for the given field.
     *
     * @param field the field to aggregate on
     */
    public MongoAvgFacet(Mapping field) {
        this(field.toString(), field);
    }

    /**
     * Specifies the callback to invoke once the facet has been computed completely.
     *
     * @param completionCallback the callback to invoke
     * @return the facet itself for fluent method calls
     */
    public MongoAvgFacet onComplete(Consumer<MongoAvgFacet> completionCallback) {
        this.completionCallback = completionCallback;
        return this;
    }

    @Override
    public void emitFacets(EntityDescriptor descriptor, BiConsumer<String, DBObject> facetConsumer) {
        BasicDBList facet = new BasicDBList();
        String fieldName = descriptor.findProperty(field.toString()).getPropertyName();
        facet.add(new BasicDBObject().append("$group",
                                             new BasicDBObject().append("_id", null)
                                                                .append("avg",
                                                                        new BasicDBObject().append("$avg",
                                                                                                   "$" + fieldName))));
        facetConsumer.accept(name, facet);
    }

    @Override
    public void digest(Doc result) {
        result.getList(name)
              .stream()
              .findFirst()
              .map(Document.class::cast)
              .ifPresent(group -> this.value = Value.of(group.get("avg")).getAmount());

        if (completionCallback != null) {
            completionCallback.accept(this);
        }
    }

    public Amount getValue() {
        return value;
    }
}
