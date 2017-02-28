/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import com.google.common.collect.Lists;
import sirius.db.jdbc.Database;
import sirius.db.jdbc.Row;
import sirius.db.jdbc.SQLQuery;
import sirius.kernel.async.Future;
import sirius.kernel.commons.Context;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;
import sirius.kernel.health.Log;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The <tt>Object Model Access</tt> provides an entity object based persistence framework.
 * <p>
 * This is the central class to {@link #find(Class, Object)}, {@link #select(Class)}, {@link #update(Entity)} and
 * {@link
 * #delete(Entity)} entitties.
 */
@Register(classes = OMA.class)
public class OMA {

    public static final Log LOG = Log.get("oma");
    private static final String SQL_WHERE_ID = " WHERE id = ?";
    private static final String SQL_AND_VERSION = " AND version = ?";

    @Part
    private Schema schema;

    private Boolean ready;

    /**
     * Provides the underlying database instance used to perform the actual statements.
     *
     * @return the database used by the framework
     */
    public Database getDatabase() {
        return schema.getDatabase();
    }

    /**
     * Provides a {@link Future} which is fullfilled once the framework is ready.
     *
     * @return a future which can be used to delay startup actions until the framework is fully functional.
     */
    public Future getReadyFuture() {
        return schema.getReadyFuture();
    }

    /**
     * Determines if the framework is ready yet.
     *
     * @return <tt>true</tt> if the framework is ready, <tt>false</tt> otherwise.
     */
    public boolean isReady() {
        if (ready == null) {
            if (schema == null) {
                return false;
            }
            ready = Boolean.FALSE;
            getReadyFuture().onSuccess(o -> ready = Boolean.TRUE);
        }

        return ready.booleanValue();
    }

    /**
     * Writes the contents of the given entity to the database.
     * <p>
     * If the entity is not persisted yet, we perform an insert. If the entity does exist, we only
     * update those fields, which were changed since they were last read from the database.
     * <p>
     * While this provides the best performance and circumvents update conflicts, it does not guarantee strong
     * consistency as the fields in the database might have partially changes. If this behaviour is unwanted, the
     * entity should be marked with {@link sirius.db.mixing.annotations.Versioned} which will turn on <tt>Optimistic
     * Locking</tt> and prevent these conditions.
     *
     * @param entity the entity to write to the database
     * @param <E>    the generic type of the entity
     */
    public <E extends Entity> void update(E entity) {
        try {
            doUpdate(entity, false);
        } catch (OptimisticLockException e) {
            throw Exceptions.handle(e);
        }
    }

    /**
     * Tries to perform an {@link #update(Entity)} of the given entity.
     * <p>
     * If the entity is {@link sirius.db.mixing.annotations.Versioned} and the entity was modified already
     * elsewhere, an {@link OptimisticLockException} will be thrown, which can be used to trigger a retry.
     *
     * @param entity the entity to update
     * @param <E>    the generic type of the entity
     * @throws OptimisticLockException in case of a concurrent modification
     */
    public <E extends Entity> void tryUpdate(E entity) throws OptimisticLockException {
        doUpdate(entity, false);
    }

    /**
     * Performs an {@link #update(Entity)} of the entity, without checking for concurrent modifications.
     * <p>Concurrent modifications by other users will simply be ignored and overridden.
     *
     * @param entity the entity to update
     * @param <E>    the generic type of the entity
     */
    public <E extends Entity> void override(E entity) {
        try {
            doUpdate(entity, true);
        } catch (OptimisticLockException e) {
            // Should really not happen....
            throw Exceptions.handle(e);
        }
    }

    @SuppressWarnings("unchecked")
    private <E extends Entity> void doUpdate(E entity, boolean force) throws OptimisticLockException {
        if (entity == null) {
            return;
        }

        try {
            EntityDescriptor ed = entity.getDescriptor();
            ed.beforeSave(entity);

            if (entity.isNew()) {
                executeINSERT(entity, ed);
            } else {
                executeUPDATE(entity, force, ed);
            }

            ed.afterSave(entity);
        } catch (OptimisticLockException e) {
            throw e;
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(LOG)
                            .error(e)
                            .withSystemErrorMessage("Unable to UPDATE %s (%s - %s): %s (%s)",
                                                    entity,
                                                    entity.getClass().getSimpleName(),
                                                    entity.getId())
                            .handle();
        }
    }

    private <E extends Entity> void executeINSERT(E entity, EntityDescriptor ed) throws SQLException {
        Context insertData = Context.create();
        for (Property p : ed.getProperties()) {
            insertData.set(p.getColumnName(), p.getValueForColumn(entity));
        }
        if (ed.isVersioned()) {
            insertData.put("version", entity.getVersion());
        }
        Row keys = getDatabase().insertRow(ed.getTableName(), insertData);
        if (keys.hasValue("id")) {
            // Normally the name of the generated column is reported
            entity.setId(keys.getValue("id").asLong(-1));
        } else if (keys.hasValue("GENERATED_KEY")) {
            // however MySQL reports "GENERATED_KEY"...
            entity.setId(keys.getValue("GENERATED_KEY").asLong(-1));
        }
    }

    private <E extends Entity> void executeUPDATE(E entity, boolean force, EntityDescriptor ed)
            throws SQLException, OptimisticLockException {
        StringBuilder sql = new StringBuilder("UPDATE ");
        sql.append(ed.getTableName());
        List<Object> data = Lists.newArrayList();
        sql.append(" SET ");
        boolean fieldUpdated = false;
        for (Property p : ed.getProperties()) {
            if (ed.isChanged(entity, p)) {
                if (!data.isEmpty()) {
                    sql.append(", ");
                }
                sql.append(p.getColumnName());
                sql.append(" = ? ");
                data.add(p.getValueForColumn(entity));
                fieldUpdated = true;
            }
        }

        if (!fieldUpdated) {
            return;
        }

        if (ed.isVersioned()) {
            if (!data.isEmpty()) {
                sql.append(",");
            }
            sql.append("version = ? ");
        }

        sql.append(SQL_WHERE_ID);
        if (ed.isVersioned() && !force) {
            sql.append(SQL_AND_VERSION);
        }
        executeUPDATE(entity, force, ed, sql.toString(), data);
    }

    private <E extends Entity> void executeUPDATE(E entity,
                                                  boolean force,
                                                  EntityDescriptor ed,
                                                  String sql,
                                                  List<Object> data) throws SQLException, OptimisticLockException {
        try (Connection c = getDatabase().getConnection()) {
            try (PreparedStatement stmt = c.prepareStatement(sql)) {
                int index = 1;
                for (Object o : data) {
                    stmt.setObject(index++, o);
                }
                if (ed.isVersioned()) {
                    stmt.setInt(index++, ed.getVersion(entity) + 1);
                }
                stmt.setLong(index++, entity.getId());
                if (ed.isVersioned() && !force) {
                    stmt.setInt(index++, ed.getVersion(entity));
                }
                int updatedRows = stmt.executeUpdate();
                enforceUpdate(entity, force, updatedRows);

                ed.setVersion(entity, ed.getVersion(entity) + 1);
            }
        }
    }

    private <E extends Entity> void enforceUpdate(E entity, boolean force, int updatedRows)
            throws OptimisticLockException {
        if (force || updatedRows > 0) {
            return;
        }
        if (find(entity.getClass(), entity.getId()).isPresent()) {
            throw new OptimisticLockException();
        } else {
            throw Exceptions.handle()
                            .to(LOG)
                            .withSystemErrorMessage(
                                    "The entity %s (%s) cannot be updated as it does not exist in the database!",
                                    entity,
                                    entity.getId())
                            .handle();
        }
    }

    /**
     * Deletes the given entity from the database.
     * <p>
     * If the entity is {@link sirius.db.mixing.annotations.Versioned} and concurrently modified elsewhere,
     * an exception is thrown.
     *
     * @param entity the entity to delete
     * @param <E>    the generic entity type
     */
    public <E extends Entity> void delete(E entity) {
        try {
            doDelete(entity, false);
        } catch (OptimisticLockException e) {
            throw Exceptions.handle(e);
        }
    }

    /**
     * Tries to delete the entity from the database.
     * <p>
     * If the entity is {@link sirius.db.mixing.annotations.Versioned} and concurrently modified elsewhere,
     * an {@link OptimisticLockException} is thrown.
     *
     * @param entity the entity to delete
     * @param <E>    the generic entity type
     * @throws OptimisticLockException if the entity was concurrently modified
     */
    public <E extends Entity> void tryDelete(E entity) throws OptimisticLockException {
        doDelete(entity, false);
    }

    /**
     * Deletes the given entity from the database even if it is {@link sirius.db.mixing.annotations.Versioned} and was
     * concurrently modified.
     *
     * @param entity the entity to delete
     * @param <E>    the generic entity type
     */
    public <E extends Entity> void forceDelete(E entity) {
        try {
            doDelete(entity, true);
        } catch (OptimisticLockException e) {
            // Should really not happen....
            throw Exceptions.handle(e);
        }
    }

    private <E extends Entity> void doDelete(E entity, boolean force) throws OptimisticLockException {
        if (entity == null || entity.isNew()) {
            return;
        }

        try {
            EntityDescriptor ed = entity.getDescriptor();
            ed.beforeDelete(entity);
            StringBuilder sb = new StringBuilder("DELETE FROM ");
            sb.append(ed.getTableName());
            sb.append(SQL_WHERE_ID);
            if (ed.isVersioned() && !force) {
                sb.append(SQL_AND_VERSION);
            }
            execDelete(entity, force, ed, sb.toString());
            ed.afterDelete(entity);
        } catch (OptimisticLockException e) {
            throw e;
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(LOG)
                            .error(e)
                            .withSystemErrorMessage("Unable to DELETE %s (%s - %s): %s (%s)",
                                                    entity,
                                                    entity.getClass().getSimpleName(),
                                                    entity.getId())
                            .handle();
        }
    }

    private <E extends Entity> void execDelete(E entity, boolean force, EntityDescriptor ed, String sql)
            throws SQLException, OptimisticLockException {
        try (Connection c = getDatabase().getConnection()) {
            try (PreparedStatement stmt = c.prepareStatement(sql)) {
                stmt.setLong(1, entity.getId());
                if (ed.isVersioned() && !force) {
                    stmt.setInt(2, ed.getVersion(entity));
                }
                int updatedRows = stmt.executeUpdate();
                if (updatedRows == 0 && find(entity.getClass(), entity.getId()).isPresent()) {
                    throw new OptimisticLockException();
                }
            }
        }
    }

    /**
     * Determines if the given entity has validation warnings.
     *
     * @param entity the entity to check
     * @return <tt>true</tt> if there are validation warnings, <tt>false</tt> otherwise
     */
    public boolean hasValidationWarnings(Entity entity) {
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
    public List<String> validate(Entity entity) {
        if (entity == null) {
            return Collections.emptyList();
        }

        EntityDescriptor ed = entity.getDescriptor();
        return ed.validate(entity);
    }

    /**
     * Creates a {@link SmartQuery} for the given entity type.
     *
     * @param type the type of entities to select
     * @param <E>  the generic type of entities to select
     * @return a query for entities of the given type
     */
    public <E extends Entity> SmartQuery<E> select(Class<E> type) {
        return new SmartQuery<>(type, getDatabase());
    }

    /**
     * Transforms a plain {@link SQLQuery} to directly return entities of the given type.
     *
     * @param type the type of entites to read from the query result
     * @param qry  the query to transform
     * @param <E>  the generic type of entities to read from the query result
     * @return a transformed query which returns entities instead of result rows.
     */
    public <E extends Entity> TransformedQuery<E> transform(Class<E> type, SQLQuery qry) {
        return new TransformedQuery<>(type, null, qry);
    }

    /**
     * Same as {@link #transform(Class, SQLQuery)} but with support for aliased columns.
     * <p>
     * If the columns to read from the result set are aliased, this method can be used to specify the alias to use.
     *
     * @param type  the type of entites to read from the query result
     * @param alias the alias which is appended to all column names to readl
     * @param qry   the query to transform
     * @param <E>   the generic type of entities to read from the query result
     * @return a transformed query which returns entities instead of result rows.
     */
    public <E extends Entity> TransformedQuery<E> transform(Class<E> type, String alias, SQLQuery qry) {
        return new TransformedQuery<>(type, alias, qry);
    }

    /**
     * Performs a database lookup to select the entity of the given type with the given id.
     *
     * @param type the type of entity to select
     * @param id   the id (which can be either a long, Long or String) to select
     * @param <E>  the generic type of the entity to select
     * @return the entity wrapped as <tt>Optional</tt> or an empty optional if no entity with the given id exists
     */
    public <E extends Entity> Optional<E> find(Class<E> type, Object id) {
        try {
            EntityDescriptor ed = schema.getDescriptor(type);
            try (Connection c = getDatabase().getConnection()) {
                return execFind(id, ed, c);
            }
        } catch (HandledException e) {
            throw e;
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(LOG)
                            .error(e)
                            .withSystemErrorMessage("Unable to FIND  %s (%s): %s (%s)", type.getSimpleName(), id)
                            .handle();
        }
    }

    @SuppressWarnings("unchecked")
    protected <E extends Entity> Optional<E> execFind(Object id, EntityDescriptor ed, Connection c) throws Exception {
        try (PreparedStatement stmt = c.prepareStatement("SELECT * FROM " + ed.getTableName() + SQL_WHERE_ID,
                                                         ResultSet.TYPE_FORWARD_ONLY,
                                                         ResultSet.CONCUR_READ_ONLY)) {
            stmt.setLong(1, Value.of(id).asLong(-1));
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    Set<String> columns = SmartQuery.readColumns(rs);
                    Entity entity = ed.readFrom(null, columns, rs);
                    return Optional.of((E) entity);
                } else {
                    return Optional.empty();
                }
            }
        }
    }

    /**
     * Tries to {@link #find(Class, Object)} the entity with the given id.
     * <p>
     * If no entity is found, an exception is thrown.
     *
     * @param type the type of entity to select
     * @param id   the id (which can be either a long, Long or String) to select
     * @param <E>  the generic type of the entity to select
     * @return the entity with the given id
     * @throws HandledException if no entity with the given ID was present
     */
    public <E extends Entity> E findOrFail(Class<E> type, Object id) {
        Optional<E> result = find(type, id);
        if (result.isPresent()) {
            return result.get();
        } else {
            throw Exceptions.handle()
                            .to(LOG)
                            .withSystemErrorMessage("Cannot find entity of type '%s' with id '%s'", type.getName(), id)
                            .handle();
        }
    }

    /**
     * Tries to resolve the {@link Entity#getUniqueName()} into an entity.
     *
     * @param name the name of the entity to resolve
     * @return the resolved entity wrapped as <tt>Optional</tt> or an empty optional if no such entity exists
     */
    @SuppressWarnings("unchecked")
    public <E extends Entity> Optional<E> resolve(String name) {
        if (Strings.isEmpty(name)) {
            return Optional.empty();
        }

        Tuple<String, String> typeAndId = Schema.splitUniqueName(name);
        return find((Class<E>) schema.getDescriptor(typeAndId.getFirst()).getType(), typeAndId.getSecond());
    }

    /**
     * Tries to {@link #resolve(String)} the given name into an entity.
     *
     * @param name the name of the entity to resolve
     * @return the resolved entity
     * @throws HandledException if the given name cannot be resolved into an entity
     */
    public Entity resolveOrFail(String name) {
        Optional<? extends Entity> result = resolve(name);
        if (result.isPresent()) {
            return result.get();
        } else {
            throw Exceptions.handle().to(LOG).withSystemErrorMessage("Cannot find entity named '%s'", name).handle();
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
    public <E extends Entity> E tryRefresh(E entity) {
        if (entity != null) {
            Optional<E> result = find((Class<E>) entity.getClass(), entity.getId());
            if (result.isPresent()) {
                return result.get();
            }
        }
        return entity;
    }

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
    public <E extends Entity> E refreshOrFail(E entity) {
        if (entity == null) {
            return null;
        }
        Optional<E> result = find((Class<E>) entity.getClass(), entity.getId());
        if (result.isPresent()) {
            return result.get();
        } else {
            throw Exceptions.handle()
                            .to(LOG)
                            .withSystemErrorMessage(
                                    "Cannot refresh entity '%s' of type '%s' (entity cannot be found in the database)",
                                    entity,
                                    entity.getClass())
                            .handle();
        }
    }
}
