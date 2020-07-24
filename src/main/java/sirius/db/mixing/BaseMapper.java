/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.jdbc.SQLEntity;
import sirius.db.mixing.annotations.Versioned;
import sirius.db.mixing.query.Query;
import sirius.db.mixing.query.constraints.Constraint;
import sirius.db.mixing.query.constraints.FilterFactory;
import sirius.db.mixing.types.BaseEntityRef;
import sirius.kernel.Sirius;
import sirius.kernel.async.TaskContext;
import sirius.kernel.commons.Callback;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Monoflop;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.UnitOfWork;
import sirius.kernel.commons.Value;
import sirius.kernel.commons.Wait;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;

import javax.annotation.CheckReturnValue;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Declares the common functionality of a mapper which is responsible for storing and loading entities to and from a database.
 *
 * @param <B> the type of entities supported by this mapper
 * @param <C> the type of constraints supported by this mapper
 * @param <Q> the type of queries supported by this mapper
 */
public abstract class BaseMapper<B extends BaseEntity<?>, C extends Constraint, Q extends Query<?, ? extends B, C>> {

    private static final Function<String, Value> EMPTY_CONTEXT = key -> Value.EMPTY;

    /**
     * Contains the name of the version column used for optimistic locking.
     */
    public static final String VERSION = "version";

    @Part
    protected Mixing mixing;

    /**
     * Writes the contents of the given entity to the database.
     * <p>
     * If the entity is not persisted yet, we perform an insert. If the entity does exist, we only
     * update those fields, which were changed since they were last read from the database.
     * <p>
     * While this provides the best performance and circumvents update conflicts, it does not guarantee strong
     * consistency as the fields in the database might have partially changes. If this behaviour is unwanted,
     * {@link Versioned} can be used which will turn on <tt>Optimistic Locking</tt> and
     * prevent these conditions.
     *
     * @param entity the entity to write to the database
     * @param <E>    the generic type of the entity
     */
    public <E extends B> void update(E entity) {
        try {
            performUpdate(entity, false);
        } catch (OptimisticLockException | IntegrityConstraintFailedException e) {
            throw Exceptions.handle(e);
        }
    }

    /**
     * Tries to perform an {@link #update(BaseEntity)} of the given entity.
     * <p>
     * If the entity is {@link Versioned} and the entity was modified already elsewhere, an
     * {@link OptimisticLockException} will be thrown, which can be used to trigger a retry.
     *
     * @param entity the entity to update
     * @param <E>    the generic type of the entity
     * @throws OptimisticLockException            in case of a concurrent modification
     * @throws IntegrityConstraintFailedException in case of a failed integrity constraint as signaled by the database
     */
    @SuppressWarnings("squid:S1160")
    @Explain("In this case we want to throw two distinct exceptions to differentiate between our optimistic locking "
             + "and database supported OL")
    public <E extends B> void tryUpdate(E entity) throws OptimisticLockException, IntegrityConstraintFailedException {
        performUpdate(entity, false);
    }

    /**
     * Tries to apply the given changes and to save the resulting entity.
     * <p>
     * Tries to perform the given modifications and then to update the entity. If an optimistic lock error occurs,
     * the entity is refreshed and the modifications are re-executed along with another update.
     *
     * @param entity          the entity to update
     * @param preSaveModifier the changes to perform on the entity
     * @param <E>             the type of the entity to update
     * @throws HandledException if either any other exception occurs, or if all three attempts fail with an optimistic
     *                          lock error.
     */
    public <E extends B> void retryUpdate(E entity, Callback<E> preSaveModifier) {
        Monoflop retryOccured = Monoflop.create();
        retry(() -> {
            E entityToUpdate = entity;
            if (retryOccured.successiveCall()) {
                entityToUpdate = tryRefresh(entity);
            }

            preSaveModifier.invoke(entityToUpdate);
            tryUpdate(entityToUpdate);
        });
    }

    /**
     * Handles the given unit of work while restarting it if an optimistic lock error occurs.
     *
     * @param unitOfWork the unit of work to handle.
     * @throws HandledException if either any other exception occurs, or if all three attempts
     *                          fail with an optimistic lock error.
     */
    public void retry(UnitOfWork unitOfWork) {
        int retries = 3;
        while (retries > 0) {
            retries--;
            try {
                unitOfWork.execute();
                return;
            } catch (OptimisticLockException e) {
                Mixing.LOG.FINE(e);
                if (Sirius.isDev()) {
                    Mixing.LOG.INFO("Retrying due to optimistic lock: %s", e);
                }
                if (retries <= 0) {
                    throw Exceptions.handle()
                                    .withSystemErrorMessage(
                                            "Failed to update an entity after re-trying a unit of work several times: %s (%s)")
                                    .error(e)
                                    .to(Mixing.LOG)
                                    .handle();
                }
                int timeoutFactor = determineRetryTimeoutFactor();
                // Wait 0, x ms, 2*x ms
                Wait.millis((2 - retries) * timeoutFactor);
                // Wait 0..x ms in 50% of all calls...
                Wait.randomMillis(-timeoutFactor, timeoutFactor);
            } catch (HandledException e) {
                throw e;
            } catch (Exception e) {
                throw Exceptions.handle()
                                .withSystemErrorMessage(
                                        "An unexpected exception occurred while executing a unit of work: %s (%s)")
                                .error(e)
                                .to(Mixing.LOG)
                                .handle();
            }
        }
    }

    /**
     * Determines the factor in ms to be used by {@link #retry} for specifying how long should be waited between retries.
     *
     * @return an amount of ms which should be used as basis for calculating the retry timeout
     */
    protected abstract int determineRetryTimeoutFactor();

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
        } catch (IntegrityConstraintFailedException | OptimisticLockException e) {
            throw Exceptions.handle(e);
        }
    }

    @SuppressWarnings("squid:RedundantThrowsDeclarationCheck")
    @Explain("false positive - both exceptions can be thrown")
    protected <E extends B> void performUpdate(E entity, boolean force)
            throws OptimisticLockException, IntegrityConstraintFailedException {
        if (entity == null) {
            return;
        }

        try {
            EntityDescriptor ed = entity.getDescriptor();
            ed.beforeSave(entity);

            if (entity.isNew()) {
                createEntity(entity, ed);
            } else {
                updateEntity(entity, force, ed);
            }

            ed.afterSave(entity);
        } catch (IntegrityConstraintFailedException | OptimisticLockException e) {
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

    /**
     * Creates a new entity in the underlying database.
     *
     * @param entity the entity to create
     * @param ed     the descriptor of the entity
     * @throws Exception in case of an database error
     */
    protected abstract void createEntity(B entity, EntityDescriptor ed) throws Exception;

    /**
     * Updates an existing entity in the underlying database.
     *
     * @param entity the entity to update
     * @param force  <tt>ture</tt> if the update is forced and optimistic locking must be disabled
     * @param ed     the descriptor of the entity
     * @throws Exception in case of an database error
     */
    protected abstract void updateEntity(B entity, boolean force, EntityDescriptor ed) throws Exception;

    /**
     * Deletes the given entity from the database.
     * <p>
     * If the entity is {@link Versioned} and concurrently modified elsewhere,
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
     * If the entity is {@link Versioned} and concurrently modified elsewhere,
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
     * Deletes the given entity from the database even if it is {@link Versioned} and was
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
            if (TaskContext.get().isActive()) {
                deleteEntity(entity, force, ed);
                ed.afterDelete(entity);
            }
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

    /**
     * Deletes the give entity from the database.
     *
     * @param entity the entity to delete
     * @param force  <tt>true</tt> if the delete is forced and optimistic locking must be disabled
     * @param ed     the descriptor of the entity
     * @throws Exception in case of an database error
     */
    protected abstract void deleteEntity(B entity, boolean force, EntityDescriptor ed) throws Exception;

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
     * @param info info provided as context (e.g. routing infos for Elasticsearch)
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

    /**
     * Tries to find the entity with the given id.
     *
     * @param id      the id of the entity to find
     * @param ed      the descriptor of the entity to find
     * @param context the advanced search context which can be populated using {@link ContextInfo} in
     *                {@link #find(Class, Object, ContextInfo...)}
     * @param <E>     the effective type of the entity
     * @return the entity wrapped as optional or an empty optional if the entity was not found
     * @throws Exception in case of a database error
     */
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
     * @param info info provided as context (e.g. routing infos for Elasticsearch)
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
     * @param info info provided as context (e.g. routing infos for Elasticsearch)
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
     * @param info info provided as context (e.g. routing infos for Elasticsearch)
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
    @CheckReturnValue
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
    @CheckReturnValue
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

    /**
     * Creates a query for the given type.
     *
     * @param type the type of entities to query for.
     * @param <E>  the generic type of entities to be returned
     * @return a query used to search for entities of the given type
     */
    public abstract <E extends B> Q select(Class<E> type);

    /**
     * Returns the filter factory which is used by this mapper.
     *
     * @return the filter factory used by this mapper
     */
    public abstract FilterFactory<C> filters();

    /**
     * Provides the most efficient way of retrieving the field value of the requested entity.
     * <p>
     * Note that it is probably advisable to not call this method directly but rather
     * {@link FieldLookupCache#lookup(BaseEntityRef, String)} which provides a cache.
     *
     * @param type  the type of the entity
     * @param id    the id of the entity
     * @param field the field to resolve
     * @return the field value, transformed into the appropriate type
     * @throws Exception in case of an error during a lookup
     */
    public abstract Value fetchField(Class<? extends B> type, Object id, Mapping field) throws Exception;
}
