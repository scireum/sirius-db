/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import sirius.db.util.BaseEntityCache;
import sirius.kernel.di.std.Part;

/**
 * Provides a template for caching mongo entities in an on-heap cache.
 *
 * @param <E> the type of entities being cached
 */
public abstract class MongoEntityCache<E extends MongoEntity> extends BaseEntityCache<String, E> {

    @Part
    protected Mango mango;

    @Override
    protected E fetchFromDb(String id) {
        return mango.find(getEntityClass(), id).orElse(null);
    }
}
