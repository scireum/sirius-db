/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import sirius.db.jdbc.Capability;
import sirius.db.jdbc.Database;
import sirius.db.jdbc.Databases;
import sirius.kernel.async.Future;
import sirius.kernel.async.TaskContext;
import sirius.kernel.async.Tasks;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.Initializable;
import sirius.kernel.di.Injector;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.mixing.schema.DatabaseDialect;
import sirius.mixing.schema.SchemaTool;
import sirius.mixing.schema.SchemaUpdateAction;
import sirius.mixing.schema.Table;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Register(classes = {Schema.class, Initializable.class})
public class Schema implements Initializable {

    private Map<Class<?>, EntityDescriptor> descriptorsByType = Maps.newHashMap();
    private Map<String, EntityDescriptor> descriptorsByName = Maps.newHashMap();
    private Future readyFuture = new Future();

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

    public EntityDescriptor getDescriptor(String aTypeName) {
        EntityDescriptor ed = descriptorsByName.get(aTypeName);
        if (ed == null) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .withSystemErrorMessage("The name '%s' is not a known entity!", aTypeName)
                            .handle();
        }

        return ed;
    }

    @Part
    private Databases dbs;

    @Part
    private Tasks tasks;

    private Database db;
    private Future ready = new Future();

    protected Database getDatabase() {
        if (db == null) {
            db = dbs.get(database);
        }
        return db;
    }

    @Part(configPath = "mixing.dialect")
    private DatabaseDialect dialect;

    @ConfigValue("mixing.database")
    private String database;

    @ConfigValue("mixing.updateSchema")
    private boolean updateSchema;

    private List<SchemaUpdateAction> requiredSchemaChanges = Lists.newArrayList();

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
            OMA.LOG.WARN("Successfully executed %d schema change actions...");
        } else {
            OMA.LOG.WARN("Schema is up to date, no changes required");
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
                    OMA.LOG.WARN(
                            "Warning %s uses %s as table name which is not all lowercase. This might lead to trouble with the type of DBMS you are using!",
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

    public void computeRequiredSchemaChanges() {
        try {
            List<Table> target = Lists.newArrayList();
            for (EntityDescriptor ed : descriptorsByType.values()) {
                target.add(ed.createTable());
            }
            SchemaTool tool = new SchemaTool(dialect);
            Database database = getDatabase();
            try (Connection c = database.getConnection()) {
                requiredSchemaChanges = tool.migrateSchemaTo(c, target, true);
            }
        } catch (SQLException e) {
            Exceptions.handle(OMA.LOG, e);
        }
    }

    public List<SchemaUpdateAction> getSchemaUpdateActions() {
        return Collections.unmodifiableList(requiredSchemaChanges);
    }

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
}
