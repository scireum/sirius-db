/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.util.BaseEntityCache;
import sirius.kernel.di.std.Part;

/**
 * Provides a template for caching JDBC/SQL entities in an on-heap cache.
 *
 * @param <E> the type of entities being cached
 */
public abstract class SQLEntityCache<E extends SQLEntity> extends BaseEntityCache<Long, E> {

    @Part
    protected OMA oma;

    @Override
    protected E fetchFromDb(String id) {
        return oma.find(getEntityClass(), id).orElse(null);
    }
}
