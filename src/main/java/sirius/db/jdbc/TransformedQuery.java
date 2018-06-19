/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.BaseQuery;
import sirius.db.mixing.EntityDescriptor;
import sirius.kernel.health.Exceptions;

import java.sql.SQLException;
import java.util.function.Function;

/**
 * A transformed query converts a plain {@link SQLQuery} into one that returns entities rather than rows.
 * <p>
 * This can be used to generate complex SQL queries which still use to O/R mixing to return entity objects
 * read from a query result.
 *
 * @param <E> the generic type of entities being queried
 */
public class TransformedQuery<E extends SQLEntity> extends BaseQuery<TransformedQuery<E>, E> {

    protected final String alias;
    protected final SQLQuery qry;

    protected TransformedQuery(EntityDescriptor descriptor, String alias, SQLQuery qry) {
        super(descriptor);
        this.alias = alias;
        this.qry = qry;
    }

    @Override
    public void iterate(Function<E, Boolean> handler) {
        try {
            qry.iterate(row -> {
                return invokeHandlerForRow(handler, row);
            }, getLimit());
        } catch (SQLException e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("Cannot transform a row into an entity of type '%s' for query '%s'",
                                                    descriptor.getType().getName(),
                                                    qry.toString())
                            .handle();
        }
    }

    @SuppressWarnings("unchecked")
    protected Boolean invokeHandlerForRow(Function<E, Boolean> handler, Row row) {
        try {
            E entity = (E) descriptor.make(alias, key -> row.hasValue(key) ? row.getValue(key) : null);
            if (descriptor.isVersioned()) {
                entity.setVersion(row.getValue(BaseMapper.VERSION).asInt(0));
            }
            entity.fetchRow = row;
            return handler.apply(entity);
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("Cannot transform a row into an entity of type '%s' for query '%s'",
                                                    descriptor.getType().getName(),
                                                    qry.toString())
                            .handle();
        }
    }

    @Override
    public String toString() {
        return descriptor.getType() + " [" + qry + "]";
    }
}
