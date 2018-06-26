/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.constraints;

import sirius.db.jdbc.OMA;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.query.QueryCompiler;
import sirius.db.mixing.query.QueryField;
import sirius.db.mixing.query.constraints.FilterFactory;

import java.util.List;

/**
 * Provides a query compiler for {@link sirius.db.jdbc.SmartQuery} and {@link SQLFilterFactory}.
 */
public class SQLQueryCompiler extends QueryCompiler<SQLConstraint> {

    /**
     * Creates a new instance for the given factory entity and query.
     *
     * @param factory      the factory used to create constraints
     * @param descriptor   the descriptor of entities being queried
     * @param query        the query to compile
     * @param searchFields the default search fields to query
     */
    public SQLQueryCompiler(FilterFactory<SQLConstraint> factory,
                            EntityDescriptor descriptor,
                            String query,
                            List<QueryField> searchFields) {
        super(factory, descriptor, query, searchFields);
    }

    @Override
    protected SQLConstraint compileSearchToken(Mapping field, QueryField.Mode mode, String value) {
        switch (mode) {
            case EQUAL:
                return factory.eq(field, value);
            case LIKE:
                if (value.contains("*")) {
                    return OMA.FILTERS.like(field).matches(value).ignoreCase().build();
                } else {
                    return factory.eq(field, value);
                }
            case PREFIX:
                if (value.contains("*")) {
                    return OMA.FILTERS.like(field).startsWith(value).ignoreCase().build();
                } else {
                    return OMA.FILTERS.like(field).startsWith(value).build();
                }
            default:
                return OMA.FILTERS.like(field).contains(value).ignoreCase().build();
        }
    }
}
