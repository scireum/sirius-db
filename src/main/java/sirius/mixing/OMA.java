/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing;

import com.google.common.collect.Lists;
import sirius.db.jdbc.Database;
import sirius.db.jdbc.Row;
import sirius.db.jdbc.SQLQuery;
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
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Register(classes = {OMA.class})
public class OMA {

    public static Log LOG = Log.get("oma");

    @Part
    private Schema schema;

    public Database getDatabase() {
        return schema.getDatabase();
    }

    public <E extends Entity> void update(E entity) {
        try {
            doUpdate(entity, false);
        } catch (OptimisticLockException e) {
            Exceptions.handle(e);
        }
    }

    public <E extends Entity> void tryUpdate(E entity) throws OptimisticLockException {
        doUpdate(entity, false);
    }

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
                doINSERT(entity, ed);
            } else {
                doUPDATE(entity, force, ed);
            }

            ed.afterSave(entity);
        } catch (HandledException e) {
            throw e;
        } catch (Throwable e) {
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

    private <E extends Entity> void doINSERT(E entity, EntityDescriptor ed) throws SQLException {
        Context insertData = Context.create();
        for (Property p : ed.getProperties()) {
            insertData.set(p.getColumnName(), p.getValueForColumn(entity));
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

    private <E extends Entity> void doUPDATE(E entity, boolean force, EntityDescriptor ed)
            throws SQLException, OptimisticLockException {
        StringBuilder sb = new StringBuilder("UPDATE ");
        sb.append(ed.getTableName());
        List<Object> data = Lists.newArrayList();
        sb.append(" SET ");
        for (Property p : ed.getProperties()) {
            if (!ed.isFetched(entity, p)) {
                //TODO throw exception
            }
            if (ed.isChanged(entity, p)) {
                if (!data.isEmpty()) {
                    sb.append(", ");
                }
                sb.append(p.getColumnName());
                sb.append(" = ? ");
                data.add(p.getValueForColumn(entity));
            }
        }

        if (ed.isVersioned()) {
            if (!data.isEmpty()) {
                sb.append(",");
            }
            sb.append("version = ? ");
            data.add(ed.getVersion(entity) + 1);
        }

        sb.append(" WHERE id = ?");
        if (ed.isVersioned() && !force) {
            sb.append(" AND version = ?");
        }
        try (Connection c = getDatabase().getConnection()) {
            try (PreparedStatement stmt = c.prepareStatement(sb.toString())) {
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
                if (!force && updatedRows == 0) {
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
            }
        }
    }

    public <E extends Entity> void tryDelete(E entity) throws OptimisticLockException {
        doDelete(entity, false);
    }

    public <E extends Entity> void forceDelete(E entity) {
        try {
            doDelete(entity, true);
        } catch (OptimisticLockException e) {
            // Should really not happen....
            throw Exceptions.handle(e);
        }
    }

    public <E extends Entity> void delete(E entity) {
        try {
            doDelete(entity, false);
        } catch (OptimisticLockException e) {
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
            sb.append(" WHERE id = ?");
            if (ed.isVersioned() && !force) {
                sb.append(" AND version = ?");
            }
            try (Connection c = getDatabase().getConnection()) {
                try (PreparedStatement stmt = c.prepareStatement(sb.toString())) {
                    stmt.setLong(1, entity.getId());
                    if (ed.isVersioned() && !force) {
                        stmt.setInt(2, ed.getVersion(entity));
                    }
                    int updatedRows = stmt.executeUpdate();
                    if (updatedRows == 0) {
                        if (find(entity.getClass(), entity.getId()).isPresent()) {
                            throw new OptimisticLockException();
                        }
                    }
                }
            }
            ed.afterDelete(entity);
        } catch (HandledException e) {
            throw e;
        } catch (Throwable e) {
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

    public <E extends Entity> SmartQuery<E> select(Class<E> type) {
        return new SmartQuery<>(type, getDatabase());
    }

    public <E extends Entity> TransformedQuery<E> transform(Class<E> type, SQLQuery qry) {
        return new TransformedQuery<>(type, null, qry);
    }

    public <E extends Entity> TransformedQuery<E> transform(Class<E> type, String alias, SQLQuery qry) {
        return new TransformedQuery<>(type, alias, qry);
    }

    @SuppressWarnings("unchecked")
    public <E extends Entity> Optional<E> find(Class<E> type, Object id) {
        try {
            EntityDescriptor ed = schema.getDescriptor(type);
            try (Connection c = getDatabase().getConnection()) {
                try (PreparedStatement stmt = c.prepareStatement("SELECT * FROM " + ed.getTableName() + " WHERE id = ?",
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
        } catch (HandledException e) {
            throw e;
        } catch (Throwable e) {
            throw Exceptions.handle()
                            .to(LOG)
                            .error(e)
                            .withSystemErrorMessage("Unable to FIND  %s (%s): %s (%s)", type.getSimpleName(), id)
                            .handle();
        }
    }

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

    public Optional<? extends Entity> resolve(String name) {
        if (Strings.isEmpty(name)) {
            return Optional.empty();
        }
        Tuple<String, String> typeAndId = Strings.split(name, "-");
        return find(schema.getDescriptor(typeAndId.getFirst()).getType(), typeAndId.getSecond());
    }

    public Entity resolveOrFail(String name) {
        Optional<? extends Entity> result = resolve(name);
        if (result.isPresent()) {
            return result.get();
        } else {
            throw Exceptions.handle().to(LOG).withSystemErrorMessage("Cannot find entity named '%s'", name).handle();
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
