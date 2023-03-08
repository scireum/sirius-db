/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.ContextInfo;
import sirius.db.mixing.query.Query;
import sirius.db.mixing.query.constraints.Constraint;
import sirius.kernel.commons.Value;
import sirius.kernel.health.HandledException;

import java.util.Optional;

/**
 * Expands the common functionality of the {@link BaseMapper} for databases which support calls to primary and secondary nodes.
 *
 * @param <B> the type of entities supported by this mapper
 * @param <C> the type of constraints supported by this mapper
 * @param <Q> the type of queries supported by this mapper
 */
public abstract class SecondaryCapableMapper<B extends BaseEntity<?>, C extends Constraint, Q extends Query<?, ? extends B, C>>
        extends BaseMapper<B, C, Q> {

    public static final String CONTEXT_IN_SECONDARY = "inSecondary";

    /**
     * Performs a database lookup to select the entity of the given type with the given ID.
     * <p>
     * This provides an essential boost in performance, as all nodes of the cluster are utilized. However, this
     * may return stale data if a secondary lags behind. Therefore, this data must not be stored back in the primary
     * database. This should rather only be used to serve web requests or other queries where occasional stale data
     * does no harm.
     * <p>
     * Also, this should NOT be used to fill any cache as this might poison the cache with already stale data.
     *
     * @param type the type of entity to select
     * @param id   the ID (which can be either a long, int or String) to select
     * @param <E>  the generic type of the entity to select
     * @return the entity wrapped as <tt>Optional</tt> or an empty optional if no entity with the given ID exists
     */
    public <E extends B> Optional<E> findInSecondary(Class<E> type, Object id) {
        return find(type, id, new ContextInfo(CONTEXT_IN_SECONDARY, Value.of(true)));
    }

    /**
     * Tries to {@link #findInSecondary(Class, Object)} the entity with the given ID.
     * <p>
     * If no entity is found, an exception is thrown.
     *
     * @param type the type of entity to select
     * @param id   the ID (which can be either a long, Long or String) to select
     * @param <E>  the generic type of the entity to select
     * @return the entity with the given ID
     * @throws HandledException if no entity with the given ID was present
     */
    public <E extends B> E findInSecondaryOrFail(Class<E> type, Object id) {
        return findOrFail(type, id, new ContextInfo(CONTEXT_IN_SECONDARY, Value.of(true)));
    }

    /**
     * In contrast to {@link #select(Class)} this doesn't necessarily read from the primary node but from the nearest.
     * <p>
     * This provides an essential boost in performance, as all nodes of the cluster are utilized. However, this
     * may return stale data if a secondary lags behind. Therefore, this data must not be stored back in the primary
     * database using {@link #update(B)}. This should rather only be used to serve web requests or other
     * queries where occasional stale data does no harm.
     * <p>
     * Also, this should NOT be used to fill any cache as this might poison the cache with already stale data.
     *
     * @param type the type of entities to query for
     * @param <E>  the generic type of entities to be returned
     * @return a query used to search for entities of the given type in the nearest MongoDB instance
     */
    public abstract <E extends B> Q selectFromSecondary(Class<E> type);
}
