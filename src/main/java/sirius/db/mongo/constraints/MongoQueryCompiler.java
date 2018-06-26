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
import sirius.db.mixing.query.QueryCompiler;
import sirius.db.mixing.query.QueryField;
import sirius.db.mongo.QueryBuilder;

import java.util.List;

/**
 * Provides a query compiler for {@link sirius.db.mongo.MongoQuery} and {@link MongoFilterFactory}.
 */
public class MongoQueryCompiler extends QueryCompiler<MongoConstraint> {

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
        if (mode == QueryField.Mode.EQUAL) {
            return factory.eq(field, value);
        } else {
            return QueryBuilder.FILTERS.text(value);
        }
    }
}
