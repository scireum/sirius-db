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
import sirius.db.jdbc.Database;
import sirius.db.jdbc.Databases;
import sirius.kernel.async.TaskContext;
import sirius.kernel.async.Tasks;
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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Register(classes = {Schema.class, Initializable.class})
public class Schema implements Initializable {

    private Map<Class<?>, EntityDescriptor> descriptorsByType = Maps.newHashMap();
    private Map<String, EntityDescriptor> descriptorsByName = Maps.newHashMap();

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

    public EntityDescriptor getDescriptor(String aClassName) {
        EntityDescriptor ed = descriptorsByName.get(aClassName);
        if (ed == null) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .withSystemErrorMessage("The name '%s' is not a known entity!", aClassName)
                            .handle();
        }

        return ed;
    }

    @Part
    private Databases dbs;

    @Part
    private Tasks tasks;

    private Database db;

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
        loadEntities();
        linkSchema();

        if (updateSchema) {
            updateSchemaAtStartup();
        }
    }

    protected void updateSchemaAtStartup() {
        computeRequiredSchemaChanges();
        OMA.LOG.INFO("Executing Schema Updates....");
        executeLosslessActions();
        List<SchemaUpdateAction> schemaUpdateActions = getSchemaUpdateActions();
        if (!schemaUpdateActions.isEmpty()) {
            OMA.LOG.INFO("------------------------------------------------------------");
            int successes = 0;
            for (SchemaUpdateAction action : schemaUpdateActions) {
                if (action.isDataLossPossible()) {
                    OMA.LOG.INFO("SKIPPED: " + action);
                } else if (action.isFailed()) {
                    OMA.LOG.WARN("FAILED: " + action);
                } else {
                    successes++;
                }
            }
            if (successes > 0) {
                OMA.LOG.INFO("Successfully executed %d actions...", successes);
            }
            OMA.LOG.INFO("------------------------------------------------------------");
        }
    }

    protected void loadEntities() {
        for (Entity e : Injector.context().getParts(Entity.class)) {
            EntityDescriptor ed = new EntityDescriptor(e);
            ed.initialize();
            descriptorsByType.put(e.getClass(), ed);
            String className = e.getClass().getSimpleName();
            EntityDescriptor conflictingDescriptor = descriptorsByName.get(className);
            if (conflictingDescriptor != null) {
                Exceptions.handle()
                          .to(OMA.LOG)
                          .withSystemErrorMessage(
                                  "Cannot register entity descriptor for '%s' as '%s' as this name is already taken by '%s'",
                                  e.getClass().getName(),
                                  className,
                                  conflictingDescriptor.getType().getName())
                          .handle();
            } else {
                descriptorsByName.put(className.toUpperCase(), ed);
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

    public void executeLosslessActions() {
        tasks.defaultExecutor().start(() -> {
            TaskContext ctx = TaskContext.get();
            ctx.setJobTitle("Executing Schema Changes");
            for (SchemaUpdateAction action : getSchemaUpdateActions()) {
                if (ctx.isActive() && !action.isDataLossPossible()) {
                    ctx.logAsCurrentState("Executing: %s", action.getReason());
                    action.execute(getDatabase());
                }
            }
        });
    }

    public void executeAllActions() {
        tasks.defaultExecutor().start(() -> {
            TaskContext ctx = TaskContext.get();
            ctx.setJobTitle("Executing Schema Changes");
            for (SchemaUpdateAction action : getSchemaUpdateActions()) {
                if (ctx.isActive()) {
                    ctx.logAsCurrentState("Executing: %s", action.getReason());
                    action.execute(getDatabase());
                }
            }
        });
    }
}
