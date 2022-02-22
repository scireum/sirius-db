/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.constraints;

import sirius.db.es.Elastic;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.Property;
import sirius.db.mixing.properties.BaseMapProperty;
import sirius.db.mixing.query.QueryCompiler;
import sirius.db.mixing.query.QueryField;
import sirius.db.mixing.query.constraints.FilterFactory;
import sirius.kernel.commons.Tuple;

import java.util.List;

/**
 * Provides a query compiler for {@link sirius.db.es.ElasticQuery} and {@link ElasticFilterFactory}.
 */
public class ElasticQueryCompiler extends QueryCompiler<ElasticConstraint> {

    /**
     * Creates a new instance for the given factory entity and query.
     *
     * @param factory      the factory used to create constraints
     * @param descriptor   the descriptor of entities being queried
     * @param query        the query to compile
     * @param searchFields the default search fields to query
     */
    public ElasticQueryCompiler(FilterFactory<ElasticConstraint> factory,
                                EntityDescriptor descriptor,
                                String query,
                                List<QueryField> searchFields) {
        super(factory, descriptor, query, searchFields);
    }

    @Override
    protected ElasticConstraint compileSearchToken(Mapping field, QueryField.Mode mode, String value) {
        if (mode == QueryField.Mode.EQUAL) {
            return factory.eq(field, value);
        }

        if (mode == QueryField.Mode.LIKE && !value.contains("*")) {
            return factory.eq(field, value.toLowerCase());
        }

        return Elastic.FILTERS.prefix(field, value.replace("*", "").toLowerCase());
    }

    @Override
    protected Tuple<Mapping, Property> resolvedNestedProperty(Property property, Mapping mapping, String nestedPath) {
        if (property instanceof BaseMapProperty) {
            return Tuple.create(Mapping.named(nestedPath), property);
        }

        return null;
    }

    @Override
    protected ElasticConstraint compileFieldEquals(Mapping field, Property property, FieldValue value) {
        if (value.getValue() instanceof String stringValue && property instanceof BaseMapProperty) {
            return Elastic.FILTERS.nestedMapContains(Mapping.named(property.getName()), field.getName(), stringValue);
        }

        return super.compileFieldEquals(field, property, value);
    }
}
