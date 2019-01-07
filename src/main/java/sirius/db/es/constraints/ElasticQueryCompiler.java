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
import sirius.db.mixing.query.QueryCompiler;
import sirius.db.mixing.query.QueryField;
import sirius.db.mixing.query.constraints.FilterFactory;

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
            return queryValue(field, value);
        }
        if (mode == QueryField.Mode.LIKE && !value.contains("*")) {
            return queryValue(field, value.toLowerCase());
        }

        return Elastic.FILTERS.prefix(field, value.replace("*", "").toLowerCase());
    }

    private ElasticConstraint queryValue(Mapping field, String value) {
        if (value.startsWith("-")) {
            return factory.ne(field, value.replace("-", ""));
        }
        return factory.eq(field, value);
    }
}
