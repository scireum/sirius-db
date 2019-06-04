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
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mongo.Doc;
import sirius.kernel.commons.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a term facet which aggregates a given field into a list of buckets.
 * <p>
 * This will generate a $sortByCount for the given field.
 */
public class MongoTermFacet extends MongoFacet {

    private final Mapping field;
    private List<Tuple<String, Integer>> values;

    /**
     * Generates a facet with the given name, for the given field.
     *
     * @param name  the name of the facet
     * @param field the field to aggregate on
     */
    public MongoTermFacet(String name, Mapping field) {
        super(name);
        this.field = field;
        this.values = new ArrayList<>();
    }

    /**
     * Creates a term facet for the given field.
     *
     * @param field the field to aggregate on
     */
    public MongoTermFacet(Mapping field) {
        this(field.toString(), field);
    }

    @Override
    public DBObject emitFacet(EntityDescriptor descriptor) {
        BasicDBList facet = new BasicDBList();
        String fieldName = descriptor.findProperty(field.toString()).getPropertyName();
        facet.add(new BasicDBObject().append("$sortByCount", "$" + fieldName));

        return facet;
    }

    @Override
    protected void digestResult(Doc result) {
        String term = result.get("_id").asString();
        int count = result.get("count").asInt(-1);
        values.add(Tuple.create(term, count));
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
