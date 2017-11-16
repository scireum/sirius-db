/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import sirius.db.jdbc.Capability;
import sirius.db.jdbc.Database;
import sirius.db.jdbc.Databases;
import sirius.db.mixing.schema.DatabaseDialect;
import sirius.db.mixing.schema.SchemaTool;
import sirius.db.mixing.schema.SchemaUpdateAction;
import sirius.db.mixing.schema.Table;
import sirius.kernel.async.Future;
import sirius.kernel.async.TaskContext;
import sirius.kernel.async.Tasks;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.di.Initializable;
import sirius.kernel.di.Injector;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Keeps track of all entities and their {@link EntityDescriptor}.
 * <p>
 * This does also generate/update the database schema.
 */
@Register(classes = {Schema.class, Initializable.class})
public class Schema implements Initializable {

    private Map<Class<?>, EntityDescriptor> descriptorsByType = Maps.newHashMap();
    private Map<String, EntityDescriptor> descriptorsByName = Maps.newHashMap();
    private Future readyFuture = new Future();

    @Part
    private Databases dbs;

    @Part
    private Tasks tasks;

    private Database db;

    @Part(configPath = "mixing.dialect")
    private DatabaseDialect dialect;

    @ConfigValue("mixing.database")
    private String database;

    @ConfigValue("mixing.updateSchema")
    private boolean updateSchema;

    private List<SchemaUpdateAction> requiredSchemaChanges = Lists.newArrayList();

    protected Database getDatabase() {
        if (db == null) {
            db = dbs.get(database);
        }
        return db;
    }

    /**
     * Returns the descriptor of the given entity class.
     *
     * @param aClass the entity class
     * @return the descriptor of the given entity class
     */
    public EntityDescriptor getDescriptor(Class<? extends Entity> aClass) {
        EntityDescriptor ed = descriptorsByType.get(aClass);
        if (ed == null) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .withSystemErrorMessage("The class '%s' is not a managed entity!", aClass.getName())
                            .handle();
        }

        return ed;
    }

    /**
     * Returns the descriptor for the given entity type.
     *
     * @param aTypeName a {@link EntityDescriptor#getTableName()} of an entity
     * @return the descriptor for the given type name
     * @throws sirius.kernel.health.HandledException if no matching descriptor exists
     */
    public EntityDescriptor getDescriptor(String aTypeName) {
        Optional<EntityDescriptor> ed = findDescriptor(aTypeName);
        if (!ed.isPresent()) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .withSystemErrorMessage("The name '%s' is not a known entity!", aTypeName)
                            .handle();
        }

        return ed.get();
    }

    /**
     * Returns the descriptor for the given entity type.
     *
     * @param aTypeName a {@link EntityDescriptor#getTableName()} of an entity
     * @return the descriptor for the given type name as optional
     */
    public Optional<EntityDescriptor> findDescriptor(String aTypeName) {
        return Optional.ofNullable(descriptorsByName.get(aTypeName));
    }

    /**
     * Returns all known descriptors.
     *
     * @return an unmodifyable list of all known descriptors
     */
    public Collection<EntityDescriptor> getDesciptors() {
        return descriptorsByType.values();
    }

    @Override
    public void initialize() throws Exception {
        if (dbs.hasDatabase(database)) {
            OMA.LOG.INFO("Mixing is starting up for database '%s'", database);
            loadEntities();
            linkSchema();

            if (updateSchema) {
                tasks.defaultExecutor().fork(this::updateSchemaAtStartup);
            } else {
                readyFuture.success();
            }
        } else {
            OMA.LOG.INFO("Mixing is disabled as the database '%s' is not present in the configuration...", database);
        }
    }

    /**
     * Provides a {@link Future} which is fullfilled when then schema is fully initialized
     *
     * @return the future which is fullfilled if the framework is ready
     */
    public Future getReadyFuture() {
        return readyFuture;
    }

    protected void updateSchemaAtStartup() {
        computeRequiredSchemaChanges();
        OMA.LOG.INFO("Executing Schema Updates....");
        TaskContext ctx = TaskContext.get();
        int skipped = 0;
        int executed = 0;
        int failed = 0;
        for (SchemaUpdateAction action : getSchemaUpdateActions()) {
            if (!ctx.isActive()) {
                break;
            }
            if (!action.isDataLossPossible()) {
                executed++;
                action.execute(getDatabase());
                if (action.isFailed()) {
                    failed++;
                }
            } else {
                skipped++;
            }
        }
        if (failed > 0 || skipped > 0) {
            OMA.LOG.WARN(
                    "Executed %d schema change actions of which %d failed. %d were skipped due to possible dataloss",
                    executed,
                    failed,
                    skipped);
        } else if (executed > 0) {
            OMA.LOG.INFO("Successfully executed %d schema change actions...", executed);
        } else {
            OMA.LOG.INFO("Schema is up to date, no changes required");
        }

        readyFuture.success();
    }

    protected void loadEntities() {
        for (Entity e : Injector.context().getParts(Entity.class)) {
            EntityDescriptor ed = new EntityDescriptor(e);
            ed.initialize();
            descriptorsByType.put(e.getClass(), ed);
            String typeName = e.getTypeName();
            EntityDescriptor conflictingDescriptor = descriptorsByName.get(typeName);
            if (conflictingDescriptor != null) {
                Exceptions.handle()
                          .to(OMA.LOG)
                          .withSystemErrorMessage(
                                  "Cannot register entity descriptor for '%s' as '%s' as this name is already taken by '%s'",
                                  e.getClass().getName(),
                                  typeName,
                                  conflictingDescriptor.getType().getName())
                          .handle();
            } else {
                descriptorsByName.put(typeName, ed);
            }

            if (getDatabase().hasCapability(Capability.LOWER_CASE_TABLE_NAMES)) {
                if (!Strings.areEqual(ed.getTableName(), ed.getTableName().toLowerCase())) {
                    OMA.LOG.WARN("Warning %s uses %s as table name which is not all lowercase."
                                 + " This might lead to trouble with the type of DBMS you are using!",
                                 ed.getType().getName(),
                                 ed.getTableName());
                }
            }
        }
    }

    protected void linkSchema() {
        for (EntityDescriptor ed : descriptorsByType.values()) {
            ed.link();
        }
    }

    /**
     * (Re-)Computes the changes required on the database schema to match the expectations of the software.
     * <p>
     * All non critical changes (new tables, new columns) are performed at system startup to prevent errors.
     */
    public void computeRequiredSchemaChanges() {
        try {
            List<Table> target = Lists.newArrayList();
            for (EntityDescriptor ed : descriptorsByType.values()) {
                target.add(ed.createTable());
            }
            SchemaTool tool = new SchemaTool(dialect);
            requiredSchemaChanges = tool.migrateSchemaTo(getDatabase(), target, true);
        } catch (SQLException e) {
            Exceptions.handle(OMA.LOG, e);
        }
    }

    /**
     * Lists the update actions which need to be performed on the current database to match the expectation of the
     * software.
     *
     * @return the list of required schema changes.
     */
    public List<SchemaUpdateAction> getSchemaUpdateActions() {
        return Collections.unmodifiableList(requiredSchemaChanges);
    }

    /**
     * Executes the schema change action with the given id.
     *
     * @param id the id of the action to execute
     * @return the action itself to fetch an error message in case of database problems
     */
    @Nullable
    public SchemaUpdateAction executeSchemaUpdateAction(String id) {
        for (SchemaUpdateAction a : getSchemaUpdateActions()) {
            if (Strings.areEqual(id, a.getId())) {
                a.execute(getDatabase());
                return a;
            }
        }

        return null;
    }

    /**
     * Each entity type can be addressed by its class or by a unique name, which is its simple class name in upper
     * case.
     *
     * @param type the entity class to generate the type name for
     * @param <E>  the generic type of the given class
     * @return the type name of the given type
     * @see OMA#resolve(String)
     */
    @Nonnull
    public static <E extends Entity> String getNameForType(@Nonnull Class<E> type) {
        return type.getSimpleName().toUpperCase();
    }

    /**
     * Computes the unique name of an entity based on its descriptor type and id.
     *
     * @param typeName the name of the entity type
     * @param id       the id of the entity
     * @return a unique name consisting of the typeName and id
     * @see #getNameForType(Class)
     */
    @Nonnull
    public static String getUniqueName(@Nonnull String typeName, long id) {
        return typeName + "-" + id;
    }

    /**
     * Computes the unique name of an entity based on its type and id.
     *
     * @param type the entity class to generate the type name for
     * @param id   the id of the entity
     * @return a unique name consisting of the typeName and id
     * @see #getNameForType(Class)
     */
    @Nonnull
    public static String getUniqueName(@Nonnull Class<? extends Entity> type, long id) {
        return getNameForType(type) + "-" + id;
    }

    /**
     * Splits a unique name into the descriptor type and id.
     *
     * @param uniqueName the unique name of an entity.
     * @return the type and id of the entity as tuple
     * @see #getUniqueName(String, long)
     */
    @Nonnull
    public static Tuple<String, String> splitUniqueName(@Nullable String uniqueName) {
        return Strings.split(uniqueName, "-");
    }

    /**
     * Splits a unique name into the type and numeric id.
     *
     * @param uniqueName the unique name of an entity.
     * @return the type and id of the entity as tuple
     * @throws sirius.kernel.health.HandledException if an invalid unique name was given
     * @see #getUniqueName(String, long)
     */
    @Nonnull
    public static Tuple<String, Long> parseUniqueName(@Nullable String uniqueName) {
        try {
            Tuple<String, String> typeAndId = splitUniqueName(uniqueName);
            long id = Long.parseLong(typeAndId.getSecond());
            return Tuple.create(typeAndId.getFirst(), id);
        } catch (NumberFormatException e) {
            Exceptions.ignore(e);
            throw Exceptions.createHandled()
                            .withSystemErrorMessage("Invalid entity id in unique object name: %s", uniqueName)
                            .handle();
        }
    }
}
