/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.jdbc.SQLEntity;
import sirius.db.jdbc.VersionedEntity;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public abstract class BaseMapper<B extends BaseEntity, Q extends Query<?,? extends B>> {

    private static final Function<String, Value> EMPTY_CONTEXT = key -> Value.EMPTY;

    @Part
    protected Mixing mixing;

    /**
     * Writes the contents of the given entity to the database.
     * <p>
     * If the entity is not persisted yet, we perform an insert. If the entity does exist, we only
     * update those fields, which were changed since they were last read from the database.
     * <p>
     * While this provides the best performance and circumvents update conflicts, it does not guarantee strong
     * consistency as the fields in the database might have partially changes. If this behaviour is unwanted, the
     * entity subclass {@link VersionedEntity} which will turn on <tt>Optimistic Locking</tt> and prevent these
     * conditions.
     *
     * @param entity the entity to write to the database
     * @param <E>    the generic type of the entity
     */
    public <E extends B> void update(E entity) {
        try {
            performUpdate(entity, false);
        } catch (OptimisticLockException e) {
            throw Exceptions.handle(e);
        }
    }

    /**
     * Tries to perform an {@link #update(BaseEntity)} of the given entity.
     * <p>
     * If the entity is a {@link VersionedEntity} and the entity was modified already elsewhere, an
     * {@link OptimisticLockException} will be thrown, which can be used to trigger a retry.
     *
     * @param entity the entity to update
     * @param <E>    the generic type of the entity
     * @throws OptimisticLockException in case of a concurrent modification
     */
    public <E extends B> void tryUpdate(E entity) throws OptimisticLockException {
         performUpdate(entity, false);
    }

    /**
     * Performs an {@link #update(BaseEntity)} of the entity, without checking for concurrent modifications.
     * <p>Concurrent modifications by other users will simply be ignored and overridden.
     *
     * @param entity the entity to update
     * @param <E>    the generic type of the entity
     */
    public <E extends B> void override(E entity) {
        try {
            performUpdate(entity, true);
        } catch (OptimisticLockException e) {
            // Should really not happen....
            throw Exceptions.handle(e);
        }
    }

    protected <E extends B> void performUpdate(E entity, boolean force) throws OptimisticLockException {
        if (entity == null) {
            return;
        }

        try {
            EntityDescriptor ed = entity.getDescriptor();
            ed.beforeSave(entity);

            if (entity.isNew()) {
                createEnity(entity, ed);
            } else {
                updateEntity(entity, force, ed);
            }

            ed.afterSave(entity);
        } catch (OptimisticLockException e) {
            throw e;
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .error(e)
                            .withSystemErrorMessage("Unable to UPDATE %s (%s): %s (%s)",
                                                    entity,
                                                    entity.getClass().getSimpleName())
                            .handle();
        }
    }

    protected abstract <E extends B> void createEnity(E entity, EntityDescriptor ed) throws Exception;

    protected abstract <E extends B> void updateEntity(E entity, boolean force, EntityDescriptor ed) throws Exception;

    /**
     * Deletes the given entity from the database.
     * <p>
     * If the entity is a {@link VersionedEntity} and concurrently modified elsewhere,
     * an exception is thrown.
     *
     * @param entity the entity to delete
     * @param <E>    the generic entity type
     */
    public <E extends B> void delete(E entity) {
        try {
            performDelete(entity, false);
        } catch (OptimisticLockException e) {
            throw Exceptions.handle(e);
        }
    }

    /**
     * Tries to delete the entity from the database.
     * <p>
     * If the entity is a {@link VersionedEntity} and concurrently modified elsewhere,
     * an {@link OptimisticLockException} is thrown.
     *
     * @param entity the entity to delete
     * @param <E>    the generic entity type
     * @throws OptimisticLockException if the entity was concurrently modified
     */
    public <E extends B> void tryDelete(E entity) throws OptimisticLockException {
        performDelete(entity, false);
    }

    /**
     * Deletes the given entity from the database even if it is a {@link VersionedEntity} and was
     * concurrently modified.
     *
     * @param entity the entity to delete
     * @param <E>    the generic entity type
     */
    public <E extends B> void forceDelete(E entity) {
        try {
            performDelete(entity, true);
        } catch (OptimisticLockException e) {
            // Should really not happen....
            throw Exceptions.handle(e);
        }
    }

    protected <E extends B> void performDelete(E entity, boolean force) throws OptimisticLockException {
        if (entity == null || entity.isNew()) {
            return;
        }

        try {
            EntityDescriptor ed = entity.getDescriptor();
            ed.beforeDelete(entity);
            deleteEntity(entity, force, ed);
            ed.afterDelete(entity);
        } catch (OptimisticLockException e) {
            throw e;
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .error(e)
                            .withSystemErrorMessage("Unable to DELETE %s (%s): %s (%s)",
                                                    entity,
                                                    entity.getClass().getSimpleName())
                            .handle();
        }
    }

    protected abstract <E extends B> void deleteEntity(E entity, boolean force, EntityDescriptor ed) throws Exception;

    /**
     * Determines if the given entity has validation warnings.
     *
     * @param entity the entity to check
     * @return <tt>true</tt> if there are validation warnings, <tt>false</tt> otherwise
     */
    public boolean hasValidationWarnings(B entity) {
        if (entity == null) {
            return false;
        }

        EntityDescriptor ed = entity.getDescriptor();
        return ed.hasValidationWarnings(entity);
    }

    /**
     * Executes all validation handlers on the given entity.
     *
     * @param entity the entity to validate
     * @return a list of all validation warnings
     */
    public List<String> validate(B entity) {
        if (entity == null) {
            return Collections.emptyList();
        }

        EntityDescriptor ed = entity.getDescriptor();
        return ed.validate(entity);
    }

    /**
     * Performs a database lookup to select the entity of the given type with the given id.
     *
     * @param type the type of entity to select
     * @param id   the id (which can be either a long, Long or String) to select
     * @param <E>  the generic type of the entity to select
     * @return the entity wrapped as <tt>Optional</tt> or an empty optional if no entity with the given id exists
     */
    public <E extends B> Optional<E> find(Class<E> type, Object id, ContextInfo... info) {
        try {
            if (Strings.isEmpty(id)) {
                return Optional.empty();
            }
            EntityDescriptor ed = mixing.getDescriptor(type);
            return findEntity(id, ed, makeContext(info));
        } catch (HandledException e) {
            throw e;
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .error(e)
                            .withSystemErrorMessage("Unable to FIND  %s (%s): %s (%s)", type.getSimpleName(), id)
                            .handle();
        }
    }

    private Function<String, Value> makeContext(ContextInfo[] info) {
        if (info == null || info.length == 0) {
            return EMPTY_CONTEXT;
        }

        return key -> {
            for (int i = 0; i < info.length; i++) {
                if (Strings.areEqual(info[i].getKey(), key)) {
                    return info[i].getValue();
                }
            }

            return Value.EMPTY;
        };
    }

    protected abstract <E extends B> Optional<E> findEntity(Object id,
                                                            EntityDescriptor ed,
                                                            Function<String, Value> context) throws Exception;

    /**
     * Tries to {@link #find(Class, Object, ContextInfo...)} the entity with the given id.
     * <p>
     * If no entity is found, an exception is thrown.
     *
     * @param type the type of entity to select
     * @param id   the id (which can be either a long, Long or String) to select
     * @param <E>  the generic type of the entity to select
     * @return the entity with the given id
     * @throws HandledException if no entity with the given ID was present
     */
    public <E extends B> E findOrFail(Class<E> type, Object id, ContextInfo... info) {
        Optional<E> result = find(type, id, info);
        if (result.isPresent()) {
            return result.get();
        } else {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .withSystemErrorMessage("Cannot find entity of type '%s' with id '%s'", type.getName(), id)
                            .handle();
        }
    }

    /**
     * Tries to resolve the {@link SQLEntity#getUniqueName()} into an entity.
     *
     * @param name the name of the entity to resolve
     * @param <E>  the generic parameter of the entity to resolve
     * @return the resolved entity wrapped as <tt>Optional</tt> or an empty optional if no such entity exists
     */
    @SuppressWarnings("unchecked")
    public <E extends B> Optional<E> resolve(String name, ContextInfo... info) {
        if (Strings.isEmpty(name)) {
            return Optional.empty();
        }

        Tuple<String, String> typeAndId = Mixing.splitUniqueName(name);
        if (Strings.isEmpty(typeAndId.getSecond())) {
            return Optional.empty();
        }

        return mixing.findDescriptor(typeAndId.getFirst())
                     .flatMap(descriptor -> find((Class<E>) descriptor.getType(), typeAndId.getSecond(), info));
    }

    /**
     * Tries to {@link #resolve(String, ContextInfo...)} the given name into an entity.
     *
     * @param name the name of the entity to resolve
     * @return the resolved entity
     * @throws HandledException if the given name cannot be resolved into an entity
     */
    public B resolveOrFail(String name, ContextInfo... info) {
        Optional<? extends B> result = resolve(name, info);
        if (result.isPresent()) {
            return result.get();
        } else {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .withSystemErrorMessage("Cannot find entity named '%s'", name)
                            .handle();
        }
    }

    /**
     * Tries to fetch a fresh (updated) instance of the given entity from the database.
     * <p>
     * If the entity does no longer exist, the given instance is returned.
     *
     * @param entity the entity to refresh
     * @param <E>    the generic type of the entity
     * @return a new instance of the given entity with the most current data from the database or the original entity,
     * if the entity does no longer exist in the database.
     */
    @SuppressWarnings("unchecked")
    public <E extends B> E tryRefresh(E entity) {
        if (entity != null) {
            Optional<E> result = findEntity(entity);
            if (result.isPresent()) {
                return result.get();
            }
        }
        return entity;
    }

    protected abstract <E extends B> Optional<E> findEntity(E entity);

    /**
     * Tries to fetch a fresh (updated) instance of the given entity from the database.
     * <p>
     * If the entity does no longer exist, an exception will be thrown.
     *
     * @param entity the entity to refresh
     * @param <E>    the generic type of the entity
     * @return a new instance of the given entity with the most current data from the database.
     * @throws HandledException if the entity no longer exists in the database.
     */
    @SuppressWarnings("unchecked")
    public <E extends B> E refreshOrFail(E entity) {
        if (entity == null) {
            return null;
        }
        Optional<E> result = findEntity(entity);
        if (result.isPresent()) {
            return result.get();
        } else {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .withSystemErrorMessage(
                                    "Cannot refresh entity '%s' of type '%s' (entity cannot be found in the database)",
                                    entity,
                                    entity.getClass())
                            .handle();
        }
    }

    public abstract <E extends B> Q select(Class<E> type);
}
