/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.jdbc.constraints.SQLConstraint;
import sirius.db.jdbc.constraints.SQLFilterFactory;
import sirius.db.jdbc.schema.Schema;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.IntegrityConstraintFailedException;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.OptimisticLockException;
import sirius.db.mixing.Property;
import sirius.db.mongo.SecondaryCapableMapper;
import sirius.kernel.async.Future;
import sirius.kernel.commons.Context;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
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
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Provides the {@link BaseMapper mapper} used to communicate with JDBC / SQL databases.
 */
@Register(classes = OMA.class)
public class OMA extends SecondaryCapableMapper<SQLEntity, SQLConstraint, SmartQuery<? extends SQLEntity>> {

    /**
     * Contains the central logger for the OMA facility.
     */
    public static final Log LOG = Log.get("oma");

    /**
     * Constrains the factory used to generate filters for a {@link SmartQuery}.
     */
    public static final SQLFilterFactory FILTERS = new SQLFilterFactory();

    private static final String SQL_WHERE_ID = " WHERE id = ?";
    private static final String SQL_AND_VERSION = " AND version = ?";

    @Part
    private Schema schema;

    @Part
    private Databases dbs;

    private Boolean ready;

    /**
     * Provides the underlying database instance used to perform the actual statements.
     * <p>
     * Note that there is a helper available which can generate efficient UPDATE statements which are bound
     * to various conditions. See {@link #updateStatement(Class)} for further information.
     *
     * @param realm the realm to determine the database for
     * @return the database used by the framework
     */
    @Nullable
    public Database getDatabase(String realm) {
        return schema.getDatabase(realm);
    }

    /**
     * Provides the underlying database instance used to perform the actual statements.
     * <p>
     * Note that there is a helper available which can generate efficient UPDATE statements which are bound
     * to various conditions. See {@link #updateStatement(Class)} for further information.
     *
     * @param entityType the entity to determine the database for
     * @return the database used by the framework
     */
    @Nullable
    public Database getDatabase(Class<? extends SQLEntity> entityType) {
        return getDatabase(mixing.getDescriptor(entityType).getRealm());
    }

    /**
     * Provides the underlying database instance which represents the local secondary copy of the main database.
     * <p>
     * In large environments the underlying JDBC database might be setup as a master-slave replication. Such a slave
     * is called a secondary copy of the database (as it might not always be fully up-to-date). However, for some
     * queries this is sufficient. Also, querying a local copy is faster and takes load from the main database.
     *
     * @param realm the realm to determine the database for
     * @return the secondary database used by the framework. If no secondary database is present or its usage is
     * disabled, the primary database is returned.
     */
    @Nullable
    public Database getSecondaryDatabase(String realm) {
        Tuple<Database, Database> primaryAndSecondary = schema.getDatabases(realm).orElse(null);
        if (primaryAndSecondary == null) {
            return null;
        }
        if (primaryAndSecondary.getSecond() != null) {
            return primaryAndSecondary.getSecond();
        }

        return primaryAndSecondary.getFirst();
    }

    /**
     * Provides the underlying database instance which represents the local secondary copy of the main database.
     * <p>
     * In large environments the underlying JDBC database might be setup as a master-slave replication. Such a slave
     * is called a secondary copy of the database (as it might not always be fully up-to-date). However, for some
     * queries this is sufficient. Also, querying a local copy is faster and takes load from the main database.
     *
     * @param entityType the entity to determine the database for
     * @return the secondary database used by the framework. If no secondary database is present or its usage is
     * disabled, the primary database is returned.
     */
    @Nullable
    public Database getSecondaryDatabase(Class<? extends SQLEntity> entityType) {
        return getSecondaryDatabase(mixing.getDescriptor(entityType).getRealm());
    }

    /**
     * Provides a {@link Future} which is fulfilled once the framework is ready.
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
            getReadyFuture().onSuccess(ignored -> ready = Boolean.TRUE);
        }

        return ready.booleanValue();
    }

    /**
     * Creates an UPDATE statement which can update one or more fields based on a given set of constraints.
     * <p>
     * This should be used to generate efficient UPDATE statements with nearly no framework overhead (this
     * is essentially a build for a prepared statement).
     *
     * @param entityType the type to update
     * @return the statement builder
     */
    public UpdateStatement updateStatement(Class<? extends SQLEntity> entityType) {
        EntityDescriptor descriptor = mixing.getDescriptor(entityType);
        return new UpdateStatement(descriptor, getDatabase(descriptor.getRealm()));
    }

    /**
     * Creates a DELETE statement which delete entities based on a given set of constraints.
     * <p>
     * This should be used to generate efficient DELETE statements with nearly no framework overhead (this
     * is essentially a build for a prepared statement).
     *
     * @param entityType the type to delete
     * @return the statement builder
     */
    public DeleteStatement deleteStatement(Class<? extends SQLEntity> entityType) {
        EntityDescriptor descriptor = mixing.getDescriptor(entityType);
        return new DeleteStatement(descriptor, getDatabase(descriptor.getRealm()));
    }

    @Override
    protected void createEntity(SQLEntity entity, EntityDescriptor entityDescriptor) throws Exception {
        Context insertData = Context.create();
        for (Property property : entityDescriptor.getProperties()) {
            if (!SQLEntity.ID.getName().equals(property.getName())) {
                insertData.set(property.getPropertyName(), property.getValueForDatasource(OMA.class, entity));
            }
        }

        if (entityDescriptor.isVersioned()) {
            insertData.set(VERSION, 1);
        }

        try {
            Row keys =
                    getDatabase(entityDescriptor.getRealm()).insertRow(entityDescriptor.getRelationName(), insertData);
            loadCreatedId(entity, keys);
            entity.setVersion(1);
        } catch (SQLIntegrityConstraintViolationException exception) {
            throw new IntegrityConstraintFailedException(exception);
        }
    }

    /**
     * Loads an auto generated ID from the given row.
     *
     * @param entity the target entity to write the ID to
     * @param keys   the row to read the ID from
     */
    public static void loadCreatedId(SQLEntity entity, Row keys) {
        if (keys.hasValue("id")) {
            // Normally the name of the generated column is reported
            entity.setId(keys.getValue("id").asLong(-1));
        } else if (keys.hasValue("GENERATED_KEY")) {
            // however MySQL reports "GENERATED_KEY"...
            entity.setId(keys.getValue("GENERATED_KEY").asLong(-1));
        } else if (keys.hasValue("INSERT_ID")) {
            // and MariaDB reports "INSERT_ID"...
            entity.setId(keys.getValue("INSERT_ID").asLong(-1));
        }
    }

    @Override
    protected void updateEntity(SQLEntity entity, boolean force, EntityDescriptor entityDescriptor) throws Exception {
        StringBuilder sql = new StringBuilder("UPDATE ");
        sql.append(entityDescriptor.getRelationName());
        sql.append(" SET ");
        List<Object> data = buildUpdateStatement(entity, entityDescriptor, sql);

        if (data.isEmpty()) {
            return;
        }

        if (entityDescriptor.isVersioned()) {
            if (!data.isEmpty()) {
                sql.append(",");
            }
            sql.append("version = ? ");
        }

        sql.append(SQL_WHERE_ID);
        if (entityDescriptor.isVersioned() && !force) {
            sql.append(SQL_AND_VERSION);
        }
        executeUPDATE(entity, entityDescriptor, force, sql.toString(), data);
    }

    private List<Object> buildUpdateStatement(SQLEntity entity, EntityDescriptor entityDescriptor, StringBuilder sql) {
        List<Object> data = new ArrayList<>();
        for (Property property : entityDescriptor.getProperties()) {
            if (entityDescriptor.isChanged(entity, property)) {
                if (SQLEntity.ID.getName().equals(property.getName())) {
                    throw new IllegalStateException("The id column of an entity must not be modified manually!");
                }
                if (!data.isEmpty()) {
                    sql.append(", ");
                }

                sql.append(property.getPropertyName());
                sql.append(" = ? ");
                data.add(property.getValueForDatasource(OMA.class, entity));
            }
        }

        return data;
    }

    private void executeUPDATE(SQLEntity entity,
                               EntityDescriptor entityDescriptor,
                               boolean force,
                               String sql,
                               List<Object> data)
            throws SQLException, OptimisticLockException, IntegrityConstraintFailedException {
        try (Connection connection = getDatabase(entityDescriptor.getRealm()).getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                int index = 1;
                for (Object object : data) {
                    Databases.convertAndSetParameter(statement, index++, object);
                }
                if (entityDescriptor.isVersioned()) {
                    statement.setInt(index++, entity.getVersion() + 1);
                }
                statement.setLong(index++, entity.getId());
                if (entityDescriptor.isVersioned() && !force) {
                    statement.setInt(index++, entity.getVersion());
                }
                int updatedRows = statement.executeUpdate();
                enforceUpdate(entity, force, updatedRows);

                if (entityDescriptor.isVersioned()) {
                    entity.setVersion(entity.getVersion() + 1);
                }
            }
        } catch (SQLIntegrityConstraintViolationException exception) {
            throw new IntegrityConstraintFailedException(exception);
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
    protected void deleteEntity(SQLEntity entity, boolean force, EntityDescriptor entityDescriptor) throws Exception {
        StringBuilder sql = new StringBuilder("DELETE FROM ");
        sql.append(entityDescriptor.getRelationName());
        sql.append(SQL_WHERE_ID);

        if (entityDescriptor.isVersioned() && !force) {
            sql.append(SQL_AND_VERSION);
        }

        try (Connection connection = getDatabase(entityDescriptor.getRealm()).getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
                statement.setLong(1, entity.getId());
                if (entityDescriptor.isVersioned() && !force) {
                    statement.setInt(2, entity.getVersion());
                }
                int updatedRows = statement.executeUpdate();
                if (updatedRows == 0 && find(entity.getClass(), entity.getId()).isPresent()) {
                    throw new OptimisticLockException();
                }
            }
        }
    }

    @Override
    public <E extends SQLEntity> SmartQuery<E> select(Class<E> type) {
        EntityDescriptor entityDescriptor = mixing.getDescriptor(type);
        return new SmartQuery<>(entityDescriptor, getDatabase(entityDescriptor.getRealm()));
    }

    @Override
    public <E extends SQLEntity> SmartQuery<E> selectFromSecondary(Class<E> type) {
        EntityDescriptor entityDescriptor = mixing.getDescriptor(type);
        return new SmartQuery<>(entityDescriptor, getSecondaryDatabase(entityDescriptor.getRealm()));
    }

    @Override
    public SQLFilterFactory filters() {
        return FILTERS;
    }

    /**
     * Transforms a plain {@link SQLQuery} to directly return entities of the given type.
     *
     * @param type  the type of entities to read from the query result
     * @param query the query to transform
     * @param <E>   the generic type of entities to read from the query result
     * @return a transformed query which returns entities instead of result rows.
     */
    public <E extends SQLEntity> TransformedQuery<E> transform(Class<E> type, SQLQuery query) {
        return new TransformedQuery<>(mixing.getDescriptor(type), null, query);
    }

    /**
     * Same as {@link #transform(Class, SQLQuery)} but with support for aliased columns.
     * <p>
     * If the columns to read from the result set are aliased, this method can be used to specify the alias to use.
     *
     * @param type  the type of entities to read from the query result
     * @param alias the alias which is appended to all column names to read
     * @param query the query to transform
     * @param <E>   the generic type of entities to read from the query result
     * @return a transformed query which returns entities instead of result rows.
     */
    public <E extends SQLEntity> TransformedQuery<E> transform(Class<E> type, String alias, SQLQuery query) {
        return new TransformedQuery<>(mixing.getDescriptor(type), alias, query);
    }

    /**
     * Tries to find the entity with the given ID
     *
     * @param id               the ID of the entity to find
     * @param entityDescriptor the descriptor of the entity to find
     * @param context          the advanced search context which can be populated using
     *                         {@link sirius.db.mixing.ContextInfo context info elements}.
     * @param <E>              the generic type of the entity tp find
     * @return the entity wrapped as optional or an empty optional if no entity was found
     * @throws Exception in case of a database or system error
     */
    @Override
    protected <E extends SQLEntity> Optional<E> findEntity(Object id,
                                                           EntityDescriptor entityDescriptor,
                                                           Function<String, Value> context) throws Exception {
        try (Connection connection = getDatabase(entityDescriptor.getRealm()).getConnection()) {
            return execFind(id, entityDescriptor, connection);
        }
    }

    @SuppressWarnings("unchecked")
    protected <E extends SQLEntity> Optional<E> execFind(Object id,
                                                         EntityDescriptor entityDescriptor,
                                                         Connection connection) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM "
                                                                       + entityDescriptor.getRelationName()
                                                                       + SQL_WHERE_ID,
                                                                       ResultSet.TYPE_FORWARD_ONLY,
                                                                       ResultSet.CONCUR_READ_ONLY)) {
            statement.setLong(1, Value.of(id).asLong(-1));
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }

                Set<String> columns = dbs.readColumns(result);
                E entity = (E) entityDescriptor.make(OMA.class, null, key -> {
                    String effectiveKey = key.toUpperCase();
                    if (!columns.contains(effectiveKey)) {
                        return null;
                    }

                    try {
                        return Value.of(result.getObject(effectiveKey));
                    } catch (SQLException exception) {
                        throw Exceptions.handle(OMA.LOG, exception);
                    }
                });

                if (entityDescriptor.isVersioned()) {
                    entity.setVersion(result.getInt(BaseMapper.VERSION.toUpperCase()));
                }

                return Optional.of(entity);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <E extends SQLEntity> Optional<E> findEntity(E entity) {
        return find((Class<E>) entity.getClass(), entity.getId());
    }

    @Override
    public Value fetchField(Class<? extends SQLEntity> type, Object id, Mapping field) throws Exception {
        if (Strings.isEmpty(id)) {
            return Value.EMPTY;
        }

        return select(type).fields(field)
                           .limit(1)
                           .eq(SQLEntity.ID, id)
                           .asSQLQuery()
                           .first()
                           .map(row -> Value.of(mixing.getDescriptor(type)
                                                      .getProperty(field)
                                                      .transformFromDatasource(getClass(),
                                                                               row.getValue(field.toString()))))
                           .orElse(Value.EMPTY);
    }

    @Override
    protected int determineRetryTimeoutFactor() {
        return 50;
    }
}
