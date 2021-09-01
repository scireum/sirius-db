/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.util;

import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.types.BaseEntityRef;
import sirius.kernel.cache.Cache;
import sirius.kernel.cache.CacheManager;
import sirius.kernel.commons.Strings;
import sirius.kernel.health.Exceptions;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Optional;

/**
 * Provides a template for caching entities using a coherent on-heap cache.
 *
 * @param <I> the type of the ID field used by the entity
 * @param <E> the type of entities being cached
 */
public abstract class BaseEntityCache<I extends Serializable, E extends BaseEntity<I>> {

    protected final Cache<String, E> entityByIdCache =
            CacheManager.createCoherentCache(getCacheName(), this::fetchFromDb, null);

    /**
     * Determines the name of the underlying cache.
     *
     * @return the name of the cache
     */
    protected abstract String getCacheName();

    /**
     * Fetches the actual entity instance from the database.
     *
     * @param id the id to fetch
     * @return the fetched entity
     */
    protected abstract E fetchFromDb(String id);

    /**
     * Determines the type of entities being cached.
     *
     * @return the type of entities being cached
     */
    protected abstract Class<E> getEntityClass();

    /**
     * Fetches the entity with the given {@link BaseEntity#ID id} from the cache and returns it.
     *
     * @param id the id of the entity to fetch
     * @return the cached entity wrapped in an Optional or an empty Optional
     */
    @Nonnull
    public Optional<E> fetchById(@Nonnull String id) {
        if (Strings.isEmpty(id)) {
            return Optional.empty();
        }
        return Optional.ofNullable(entityByIdCache.get(id));
    }

    /**
     * Fetches the entity with the given {@link BaseEntity#ID id} from the cache and returns it
     * or throws an appropriate exception when no entity could be found.
     *
     * @param id the id of the entity to fetch
     * @return the cached entity
     * @throws sirius.kernel.health.HandledException if no matching entity can be found
     */
    @Nonnull
    public E fetchRequiredById(@Nonnull String id) {
        return fetchById(id).orElseThrow(() -> Exceptions.createHandled()
                                                         .withSystemErrorMessage("Unknown %s: %s",
                                                                                 getEntityClass().getSimpleName(),
                                                                                 id)
                                                         .handle());
    }

    /**
     * Fetches the entity in the given ref from the cache and returns it.
     *
     * @param entityRef the ref pointing to the entity to fetch
     * @return the cached entity wrapped in an Optional or an empty Optional
     */
    @Nonnull
    public Optional<E> fetch(@Nonnull BaseEntityRef<I, E> entityRef) {
        if (entityRef.isValueLoaded()) {
            return entityRef.getValueIfPresent();
        }
        return fetchById(entityRef.getIdAsString());
    }

    /**
     * Fetches the entity in the given reference from the cache and returns it
     * or throws an appropriate exception when no entity could be found.
     *
     * @param entityRef the ref pointing to the entity to fetch
     * @return the cached entity
     * @throws sirius.kernel.health.HandledException if no matching entity can be found
     */
    @Nonnull
    public E fetchRequired(@Nonnull BaseEntityRef<I, E> entityRef) {
        if (entityRef.isEmpty()) {
            throw Exceptions.createHandled()
                            .withSystemErrorMessage("A required %s was expected but an empty reference was given.",
                                                    getEntityClass().getSimpleName())
                            .handle();
        }

        return fetch(entityRef).orElseThrow(() -> Exceptions.createHandled()
                                                            .withSystemErrorMessage("Unknown %s: %s",
                                                                                    getEntityClass().getSimpleName(),
                                                                                    entityRef.getIdAsString())
                                                            .handle());
    }

    /**
     * Purges the given entity from the cache.
     *
     * @param entity the entity to purge
     */
    public void remove(E entity) {
        if (entity != null && !entity.isNew()) {
            entityByIdCache.remove(entity.getIdAsString());
        }
    }
}
