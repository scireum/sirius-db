/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.constraints;

import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.Property;
import sirius.db.mixing.properties.BaseEntityRefListProperty;
import sirius.db.mixing.properties.BaseMapProperty;
import sirius.db.mixing.properties.StringListMapProperty;
import sirius.db.mixing.properties.StringListProperty;
import sirius.db.mixing.properties.StringMapProperty;
import sirius.db.mixing.query.QueryCompiler;
import sirius.db.mixing.query.QueryField;
import sirius.db.mongo.QueryBuilder;
import sirius.kernel.commons.Tuple;

import java.util.List;

/**
 * Provides a query compiler for {@link sirius.db.mongo.MongoQuery} and {@link MongoFilterFactory}.
 */
public class MongoQueryCompiler extends QueryCompiler<MongoConstraint> {

    private static final Mapping FULLTEXT_MAPPING = Mapping.named("$text");

    /**
     * Represents an artificial field which generates a search using a <tt>$text</tt> filter. Therefore
     * an appropriate <b>text</b> index has to be present.
     */
    public static final QueryField FULLTEXT = QueryField.contains(FULLTEXT_MAPPING);

    /**
     * Creates a new instance for the given factory entity and query.
     *
     * @param factory      the factory used to create constraints
     * @param descriptor   the descriptor of entities being queried
     * @param query        the query to compile
     * @param searchFields the default search fields to query
     */
    public MongoQueryCompiler(MongoFilterFactory factory,
                              EntityDescriptor descriptor,
                              String query,
                              List<QueryField> searchFields) {
        super(factory, descriptor, query, searchFields);
    }

    @Override
    protected MongoConstraint compileSearchToken(Mapping field, QueryField.Mode mode, String value) {
        if (FULLTEXT_MAPPING.equals(field)) {
            return QueryBuilder.FILTERS.text(value.toLowerCase());
        }

        if (mode == QueryField.Mode.EQUAL) {
            return factory.eq(field, value);
        } else if (mode == QueryField.Mode.PREFIX) {
            return QueryBuilder.FILTERS.prefix(field, value);
        } else {
            throw new IllegalArgumentException("MongoQueryCompiler only supports either a search in the FULLTEXT_FIELD "
                                               + "or an equals or prefix search in a string field");
        }
    }

    @Override
    protected Tuple<Mapping, Property> resolvedNestedProperty(Property property, Mapping mapping, String nestedPath) {
        if (property instanceof StringMapProperty || property instanceof StringListMapProperty) {
            return Tuple.create(mapping.join(Mapping.named(nestedPath)), property);
        }

        return null;
    }

    @Override
    protected MongoConstraint compileFieldEquals(Mapping field, Property property, FieldValue value) {
        if (value.getValue() == null && (property instanceof BaseMapProperty
                                         || property instanceof BaseEntityRefListProperty
                                         || property instanceof StringListProperty)) {
            return QueryBuilder.FILTERS.isEmptyList(field);
        }

        return super.compileFieldEquals(field, property, value);
    }
}
