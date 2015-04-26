/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing;

import sirius.db.jdbc.SQLQuery;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;

import java.sql.SQLException;
import java.util.function.Function;

/**
 * Created by aha on 26.04.15.
 */
public class TransformedQuery<E extends Entity> extends BaseQuery<E> {

    protected final String alias;
    protected final SQLQuery qry;

    @Part
    private static Schema schema;

    public TransformedQuery(Class<E> type, String alias, SQLQuery qry) {
        super(type);
        this.alias = alias;
        this.qry = qry;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void iterate(Function<E, Boolean> handler) {
        try {
            EntityDescriptor ed = schema.getDescriptor(type);
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
}
