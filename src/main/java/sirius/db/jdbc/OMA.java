/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import com.google.common.collect.Lists;
import sirius.db.jdbc.schema.Schema;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.OptimisticLockException;
import sirius.db.mixing.Property;
import sirius.kernel.async.Future;
import sirius.kernel.commons.Context;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Log;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Provides the {@link BaseMapper mapper} used to communicate with JDBC / SQL databases.
 */
@Register(classes = OMA.class)
public class OMA extends BaseMapper<SQLEntity, SmartQuery<? extends SQLEntity>> {

    public static final Log LOG = Log.get("oma");

    private static final String SQL_WHERE_ID = " WHERE id = ?";
    private static final String SQL_AND_VERSION = " AND version = ?";

    @Part
    private Schema schema;

    @Part
    private Databases dbs;

    private Boolean ready;

    /**
     * Provides the underlying database instance used to perform the actual statements.
     *
     * @param realm the realm to determine the database for
     * @return the database used by the framework
     */
    @Nullable
    public Database getDatabase(String realm) {
        return schema.getDatabase(realm);
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

    @Override
    protected void createEnity(SQLEntity entity, EntityDescriptor ed) throws Exception {
        Context insertData = Context.create();
        for (Property p : ed.getProperties()) {
            if (!SQLEntity.ID.getName().equals(p.getName())) {
                insertData.set(p.getPropertyName(), p.getValueForDatasource(entity));
            }
        }

        Row keys = getDatabase(ed.getRealm()).insertRow(ed.getRelationName(), insertData);
        loadCreatedId(entity, keys);
    }

    /**
     * Loads an auto generated id from the given row.
     *
     * @param entity the target entity to write the id to
     * @param keys   the row to read the id from
     */
    public static void loadCreatedId(SQLEntity entity, Row keys) {
        if (keys.hasValue("id")) {
            // Normally the name of the generated column is reported
            entity.setId(keys.getValue("id").asLong(-1));
        } else if (keys.hasValue("GENERATED_KEY")) {
            // however MySQL reports "GENERATED_KEY"...
            entity.setId(keys.getValue("GENERATED_KEY").asLong(-1));
        } else if (keys.hasValue("INSERT_ID")) {
            // aaand MariaDB reports "INSERT_ID"...
            entity.setId(keys.getValue("INSERT_ID").asLong(-1));
        }
    }

    @Override
    protected void updateEntity(SQLEntity entity, boolean force, EntityDescriptor ed) throws Exception {
        StringBuilder sql = new StringBuilder("UPDATE ");
        sql.append(ed.getRelationName());
        sql.append(" SET ");
        List<Object> data = buildUpdateStatement(entity, ed, sql);

        if (data.isEmpty()) {
            return;
        }

        boolean versioned = entity instanceof VersionedEntity;
        if (versioned) {
            if (!data.isEmpty()) {
                sql.append(",");
            }
            sql.append(VersionedEntity.VERSION.getName());
            sql.append(" = ? ");
        }

        sql.append(SQL_WHERE_ID);
        if (versioned && !force) {
            sql.append(SQL_AND_VERSION);
        }
        executeUPDATE(entity, ed, force, versioned, sql.toString(), data);
    }

    private List<Object> buildUpdateStatement(SQLEntity entity, EntityDescriptor ed, StringBuilder sql) {
        List<Object> data = Lists.newArrayList();
        for (Property p : ed.getProperties()) {
            if (ed.isChanged(entity, p)) {
                if (SQLEntity.ID.getName().equals(p.getName())) {
                    throw new IllegalStateException("The id column of an entity must not be modified manually!");
                }
                if (VersionedEntity.VERSION.getName().equals(p.getName())) {
                    throw new IllegalStateException("The version column of an entity must not be modified manually!");
                }
                if (!data.isEmpty()) {
                    sql.append(", ");
                }

                sql.append(p.getPropertyName());
                sql.append(" = ? ");
                data.add(p.getValueForDatasource(entity));
            }
        }

        return data;
    }

    private void executeUPDATE(SQLEntity entity,
                               EntityDescriptor ed,
                               boolean force,
                               boolean versioned,
                               String sql,
                               List<Object> data) throws SQLException, OptimisticLockException {
        try (Connection c = getDatabase(ed.getRealm()).getConnection()) {
            try (PreparedStatement stmt = c.prepareStatement(sql)) {
                int index = 1;
                for (Object o : data) {
                    stmt.setObject(index++, o);
                }
                if (versioned) {
                    stmt.setInt(index++, ((VersionedEntity) entity).getVersion() + 1);
                }
                stmt.setLong(index++, entity.getId());
                if (versioned && !force) {
                    stmt.setInt(index++, ((VersionedEntity) entity).getVersion());
                }
                int updatedRows = stmt.executeUpdate();
                enforceUpdate(entity, force, updatedRows);

                if (versioned) {
                    ((VersionedEntity) entity).setVersion(((VersionedEntity) entity).getVersion() + 1);
                }
            }
        }
    }

    private void enforceUpdate(SQLEntity entity, boolean force, int updatedRows) throws OptimisticLockException {
        if (force || updatedRows > 0) {
            return;
        }
        if (find(entity.getClass(), entity.getId()).isPresent()) {
            throw new OptimisticLockException();
        } else {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .withSystemErrorMessage(
                                    "The entity %s (%s) cannot be updated as it does not exist in the database!",
                                    entity,
                                    entity.getId())
                            .handle();
        }
    }

    @Override
    protected void deleteEntity(SQLEntity entity, boolean force, EntityDescriptor ed) throws Exception {
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        sb.append(ed.getRelationName());
        sb.append(SQL_WHERE_ID);

        boolean versioned = entity instanceof VersionedEntity;
        if (versioned && !force) {
            sb.append(SQL_AND_VERSION);
        }

        try (Connection c = getDatabase(ed.getRealm()).getConnection()) {
            try (PreparedStatement stmt = c.prepareStatement(sb.toString())) {
                stmt.setLong(1, entity.getId());
                if (versioned && !force) {
                    stmt.setInt(2, ((VersionedEntity) entity).getVersion());
                }
                int updatedRows = stmt.executeUpdate();
                if (updatedRows == 0 && find(entity.getClass(), entity.getId()).isPresent()) {
                    throw new OptimisticLockException();
                }
            }
        }
    }

    @Override
    public <E extends SQLEntity> SmartQuery<E> select(Class<E> type) {
        EntityDescriptor ed = mixing.getDescriptor(type);
        return new SmartQuery<>(ed, getDatabase(ed.getRealm()));
    }

    /**
     * Transforms a plain {@link SQLQuery} to directly return entities of the given type.
     *
     * @param type the type of entites to read from the query result
     * @param qry  the query to transform
     * @param <E>  the generic type of entities to read from the query result
     * @return a transformed query which returns entities instead of result rows.
     */
    public <E extends SQLEntity> TransformedQuery<E> transform(Class<E> type, SQLQuery qry) {
        return new TransformedQuery<>(mixing.getDescriptor(type), null, qry);
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
    public <E extends SQLEntity> TransformedQuery<E> transform(Class<E> type, String alias, SQLQuery qry) {
        return new TransformedQuery<>(mixing.getDescriptor(type), alias, qry);
    }

    /**
     * Tries to find the entity with the given id
     *
     * @param id      the id of the entity to find
     * @param ed      the descriptor of the entity to find
     * @param context the advanced search context which can be populated using
     *                {@link sirius.db.mixing.ContextInfo context info elements}.
     * @param <E>     the generic type of the entity tp find
     * @return the entity wrapped as optional or an empty optional if no entity was found
     * @throws Exception in case of a database or system error
     */
    @Override
    protected <E extends SQLEntity> Optional<E> findEntity(Object id,
                                                           EntityDescriptor ed,
                                                           Function<String, Value> context) throws Exception {
        try (Connection c = getDatabase(ed.getRealm()).getConnection()) {
            return execFind(id, ed, c);
        }
    }

    @SuppressWarnings("unchecked")
    protected <E extends SQLEntity> Optional<E> execFind(Object id, EntityDescriptor ed, Connection c)
            throws Exception {
        try (PreparedStatement stmt = c.prepareStatement("SELECT * FROM " + ed.getRelationName() + SQL_WHERE_ID,
                                                         ResultSet.TYPE_FORWARD_ONLY,
                                                         ResultSet.CONCUR_READ_ONLY)) {
            stmt.setLong(1, Value.of(id).asLong(-1));
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }

                Set<String> columns = dbs.readColumns(rs);
                E entity = (E) ed.make(null, key -> {
                    String effeciveKey = key.toUpperCase();
                    if (!columns.contains(effeciveKey)) {
                        return null;
                    }

                    try {
                        return Value.of(rs.getObject(effeciveKey));
                    } catch (SQLException e) {
                        throw Exceptions.handle(OMA.LOG, e);
                    }
                });
                return Optional.of(entity);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <E extends SQLEntity> Optional<E> findEntity(E entity) {
        return find((Class<E>) entity.getClass(), entity.getId());
    }
}
