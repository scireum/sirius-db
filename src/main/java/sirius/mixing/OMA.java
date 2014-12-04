/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing;

import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Log;
import sirius.mixing.query.Query;

import java.util.Optional;

@Register(classes = OMA.class)
public class OMA {

    private Log LOG = Log.get("oma");

    public <E extends Entity> E tryUpdate(E entity) throws OptimisticLockException {
        return null;
    }

    public <E extends Entity> E override(E entity) {
        return null;
    }

    public <E extends Entity> void tryDelete(E entity) throws OptimisticLockException {

    }

    public <E extends Entity> void forceDelete(E entity) {

    }

    public <E extends Entity> Query<E> select(Class<E> type) {
        return new Query<E>(type);
    }

    public <E extends Entity> Optional<E> find(Class<E> type, Object id) {
        return Optional.empty();
    }

    public <E extends Entity> E findOrFail(Class<E> type, Object id) {
        Optional<E> result = find(type, id);
        if (result.isPresent()) {
            return result.get();
        } else {
            throw Exceptions.handle().to(LOG).withSystemErrorMessage("Cannot find entity of type '%s' with id '%s'", type.getName(), id).handle();
        }
    }

    @SuppressWarnings("unchecked")
    public <E extends Entity> E tryRefresh(E entity) {
        if (entity != null) {
            Optional<E> result = find((Class<E>) entity.getClass(), entity.getId());
            if (result.isPresent()) {
                return result.get();
            }
        }
        return entity;
    }

    @SuppressWarnings("unchecked")
    public <E extends Entity> E refreshOrFail(E entity) {
        if (entity != null) {
            Optional<E> result = find((Class<E>) entity.getClass(), entity.getId());
            if (result.isPresent()) {
                return result.get();
            } else {
                throw Exceptions.handle().to(LOG).withSystemErrorMessage("Cannot update entity '%s' of type '%s' (entity cannot be found in the database)", entity, entity.getClass()).handle();
            }
        }
        return entity;
    }

}
