/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.jdbc.SQLQuery;
import sirius.kernel.health.Exceptions;

import java.sql.SQLException;
import java.util.function.Function;

/**
 * A transformed query converts a plain {@link SQLQuery} into one that returns entities rather than rows.
 * <p>
 * This can be used to generate complex SQL queries which still use to O/R mapper to return entity objects
 * read from a query result.
 *
 * @param <E> the generic type of entities being queried
 */
public class TransformedQuery<E extends Entity> extends BaseQuery<E> {

    protected final String alias;
    protected final SQLQuery qry;

    protected TransformedQuery(Class<E> type, String alias, SQLQuery qry) {
        super(type);
        this.alias = alias;
        this.qry = qry;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void iterate(Function<E, Boolean> handler) {
        try {
            EntityDescriptor ed = getDescriptor();
            qry.iterate(row -> {
                try {
                    return handler.apply((E) ed.readFrom(alias, row));
                } catch (Throwable e) {
                    throw Exceptions.handle()
                                    .to(OMA.LOG)
                                    .error(e)
                                    .withSystemErrorMessage(
                                            "Cannot transform a row into an entity of type '%s' for query '%s'",
                                            type.getName(),
                                            qry.toString())
                                    .handle();
                }
            }, getLimit());
        } catch (SQLException e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("Cannot transform a row into an entity of type '%s' for query '%s'",
                                                    type.getName(),
                                                    qry.toString())
                            .handle();
        }
    }

    @Override
    public String toString() {
        return type + " [" + qry + "]";
    }
}
