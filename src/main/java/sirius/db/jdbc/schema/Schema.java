/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.schema;

import com.typesafe.config.Config;
import sirius.db.jdbc.Capability;
import sirius.db.jdbc.Database;
import sirius.db.jdbc.Databases;
import sirius.db.jdbc.OMA;
import sirius.db.jdbc.SQLEntity;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.annotations.Index;
import sirius.kernel.Sirius;
import sirius.kernel.Startable;
import sirius.kernel.async.Future;
import sirius.kernel.async.TaskContext;
import sirius.kernel.async.Tasks;
import sirius.kernel.commons.MultiMap;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Wait;
import sirius.kernel.di.GlobalContext;
import sirius.kernel.di.Initializable;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.settings.Extension;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Creates the database schemata for all databases managed via {@link Mixing} and {@link OMA}.
 */
@Register(classes = {Schema.class, Startable.class, Initializable.class})
public class Schema implements Startable, Initializable {

    private static final String EXTENSION_MIXING_JDBC = "mixing.jdbc";
    private static final String KEY_DATABASE = "database";
    private static final String KEY_SECONDARY_DATABASE = "secondaryDatabase";
    private static final String KEY_SECONDARY_ENABLED = "secondaryEnabled";

    private Future readyFuture = new Future();

    @Part
    private Databases dbs;

    @Part
    private Tasks tasks;

    @Part
    private Mixing mixing;

    @Part
    private GlobalContext globalContext;

    private List<SchemaUpdateAction> requiredSchemaChanges = new ArrayList<>();
    private Map<String, Tuple<Database, Database>> databases = new HashMap<>();

    /**
     * Returns a tuple of configured databases for a given realm.
     * <p>
     * The tuple (if present) contains the primary database along with an optional secondary database.
     * This might be a local read slave which can be used for non-critical reads.
     *
     * @param realm the realm to determine the databases for
     * @return a tuple of the primary and secondary database wrapped as optional. Note that the optional might
     * be empty if no configuration is present and also note that the secondary database might be <tt>null</tt>
     * if only the primary database is configured.
     */
    @Nonnull
    public Optional<Tuple<Database, Database>> getDatabases(String realm) {
        return Optional.ofNullable(databases.get(realm));
    }

    /**
     * Provides the underlying database instance used to perform the actual statements.
     *
     * @param realm the realm to determine the database for
     * @return the database used by the framework
     */
    @Nullable
    public Database getDatabase(String realm) {
        return getDatabases(realm).map(Tuple::getFirst).orElse(null);
    }

    /**
     * Determines if a JDBC database is configured for the given realm
     *
     * @param realm the realm to check for
     * @return <tt>true</tt> if a database is configured, <tt>false</tt> otherwise
     */
    public boolean isConfigured(String realm) {
        return databases.containsKey(realm);
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
                action.execute(getDatabase(action.getRealm()));
                if (action.isFailed()) {
                    OMA.LOG.WARN("Failed schema change action - SQL: %s  reason: %s - error: %s",
                                 Strings.join(action.getSql(), "; "),
                                 action.getReason(),
                                 action.getError());
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

    /**
     * (Re-)Computes the changes required on the database schema to match the expectations of the software.
     * <p>
     * All non critical changes (new tables, new columns) are performed at system startup to prevent errors.
     */
    public void computeRequiredSchemaChanges() {
        MultiMap<String, Table> targetByRealm = MultiMap.create();
        for (EntityDescriptor ed : mixing.getDesciptors()) {
            if (SQLEntity.class.isAssignableFrom(ed.getType())) {
                if (databases.containsKey(ed.getRealm())) {
                    targetByRealm.put(ed.getRealm(), createTable(ed));
                }
            }
        }

        requiredSchemaChanges.clear();
        for (Map.Entry<String, Collection<Table>> target : targetByRealm.getUnderlyingMap().entrySet()) {
            try {
                Extension ext = Sirius.getSettings().getExtension(EXTENSION_MIXING_JDBC, target.getKey());
                if (databases.containsKey(target.getKey()) && ext.get("updateSchema").asBoolean()) {
                    SchemaTool tool = new SchemaTool(target.getKey(),
                                                     globalContext.findPart(ext.get("dialect").asString(),
                                                                            DatabaseDialect.class));
                    requiredSchemaChanges.addAll(tool.migrateSchemaTo(getDatabase(target.getKey()),
                                                                      new ArrayList<>(target.getValue()),
                                                                      true));
                }
            } catch (SQLException e) {
                Exceptions.handle(OMA.LOG, e);
            }
        }
    }

    private Table createTable(EntityDescriptor ed) {
        Table table = new Table(ed);
        table.setName(ed.getRelationName());

        if (getDatabase(ed.getRealm()).hasCapability(Capability.LOWER_CASE_TABLE_NAMES)) {
            if (!Strings.areEqual(ed.getRelationName(), ed.getRelationName().toLowerCase())) {
                OMA.LOG.WARN("Warning %s uses %s as table name which is not all lowercase."
                             + " This might lead to trouble with the type of DBMS you are using!",
                             ed.getType().getName(),
                             ed.getRelationName());
            }
        }

        collectColumns(table, ed);
        collectKeys(table, ed);

        return table;
    }

    private void applyColumnRenamings(Table table) {
        if (table.getSource().getLegacyInfo() != null && table.getSource().getLegacyInfo().hasPath("rename")) {
            Config renamedColumns = table.getSource().getLegacyInfo().getConfig("rename");
            for (TableColumn col : table.getColumns()) {
                applyRenaming(renamedColumns, col);
            }
        }
    }

    private void applyRenaming(Config renamedColumns, TableColumn col) {
        if (renamedColumns != null && renamedColumns.hasPath(col.getName())) {
            col.setOldName(renamedColumns.getString(col.getName()));
        }
    }

    private void collectKeys(Table table, EntityDescriptor ed) {
        ed.getAnnotations(Index.class).forEach(index -> {
            Key key = new Key();
            key.setName(index.name());
            for (int i = 0; i < index.columns().length; i++) {
                String name = index.columns()[i];
                Property property = ed.findProperty(name);
                if (property != null) {
                    name = property.getPropertyName();
                } else {
                    OMA.LOG.WARN("The index %s for type %s (%s) references an unknown column: %s",
                                 index.name(),
                                 ed.getType().getName(),
                                 ed.getRelationName(),
                                 name);
                }
                key.addColumn(i, name);
            }
            key.setUnique(index.unique());

            // Only add the key if the name isn't occupied already (indices are inherited from parent classes).
            // Using this approach, indices can be "overwritten" by subclasses.
            if (table.getKeys().stream().map(Key::getName).noneMatch(name -> Strings.areEqual(name, key.getName()))) {
                table.getKeys().add(key);
            }
        });
    }

    private void collectColumns(Table table, EntityDescriptor ed) {
        if (Sirius.getSettings().getExtension(EXTENSION_MIXING_JDBC, ed.getRealm()).get("createIdColumn").asBoolean()) {
            TableColumn idColumn = new TableColumn();
            idColumn.setAutoIncrement(true);
            idColumn.setName(SQLEntity.ID.getName());
            idColumn.setType(Types.BIGINT);
            idColumn.setLength(20);
            table.getColumns().add(idColumn);
            table.getPrimaryKey().add(idColumn.getName());
        }

        if (ed.isVersioned()) {
            TableColumn versionColumn = new TableColumn();
            versionColumn.setName(BaseMapper.VERSION);
            versionColumn.setType(Types.INTEGER);
            versionColumn.setLength(8);
            table.getColumns().add(versionColumn);
        }

        for (Property p : ed.getProperties()) {
            if (!(p instanceof SQLPropertyInfo)) {
                Exceptions.handle()
                          .to(OMA.LOG)
                          .withSystemErrorMessage(
                                  "The entity %s (%s) contains an unmappable property %s - SQLPropertyInfo is not available!",
                                  ed.getType().getName(),
                                  ed.getRelationName(),
                                  p.getName())
                          .handle();
            } else if (!Strings.areEqual(SQLEntity.ID.getName(), p.getName())) {
                ((SQLPropertyInfo) p).contributeToTable(table);
            }
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
                a.execute(getDatabase(a.getRealm()));
                return a;
            }
        }

        return null;
    }

    @Override
    public int getPriority() {
        return Tasks.LIFECYCLE_PRIORITY + 10;
    }

    @Override
    public void started() {
        databases.clear();
        requiredSchemaChanges.clear();

        Set<String> realms = mixing.getDesciptors()
                                   .stream()
                                   .filter(ed -> SQLEntity.class.isAssignableFrom(ed.getType()))
                                   .map(EntityDescriptor::getRealm)
                                   .collect(Collectors.toSet());

        for (String realm : realms) {
            Extension ext = Sirius.getSettings().getExtension(EXTENSION_MIXING_JDBC, realm);
            if (ext.isDefault()) {
                OMA.LOG.INFO("Mixing is disabled for realm '%s' no configuration is present...", realm);
            } else {
                String databaseName = ext.get(KEY_DATABASE).asString();
                if (dbs.hasDatabase(databaseName)) {
                    Database primary = dbs.get(databaseName);
                    databases.put(realm, Tuple.create(primary, determineSecondary(ext).orElse(primary)));
                    waitForDatabaseToBecomeReady(realm, ext.get("initSql").asString());
                } else {
                    OMA.LOG.INFO(
                            "Mixing is disabled for realm '%s' as the database '%s' is not present in the configuration...",
                            realm,
                            databaseName);
                }
            }
        }

        tasks.defaultExecutor().fork(this::updateSchemaAtStartup);
    }

    private Optional<Database> determineSecondary(Extension ext) {
        if (!ext.get(KEY_SECONDARY_ENABLED).asBoolean()) {
            return Optional.empty();
        }

        return Optional.ofNullable(dbs.get(ext.get(KEY_SECONDARY_DATABASE).asString()));
    }

    /**
     * When executing several scenarios via Docker, we observed, that especially MySQL isn't entirely ready,
     * when then port 3306 is open. Therefore we try to establish a real connection (with up to 5 retries
     * in a one...five second interval).
     */
    private void waitForDatabaseToBecomeReady(String realm, String initSql) {
        if (Strings.isEmpty(initSql)) {
            return;
        }

        int retries = 5;
        int waitInSeconds = 1;
        while (retries-- > 0) {
            Database database = getDatabase(realm);
            try (Connection connection = database.getHostConnection()) {
                executeInitialStatement(database, initSql, connection);
                return;
            } catch (SQLException e) {
                Exceptions.ignore(e);
                Wait.seconds(waitInSeconds++);
            }
        }
    }

    private void executeInitialStatement(Database database, String initSql, Connection connection) {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(initSql);
        } catch (SQLException e) {
            Exceptions.handle()
                      .to(OMA.LOG)
                      .error(e)
                      .withSystemErrorMessage(
                              "An error occured while executing the initial SQL statement for: %s (%s) - %s (%s)",
                              database.getUrl(),
                              initSql)
                      .handle();
        }
    }

    @Override
    public void initialize() throws Exception {
        readyFuture = new Future();
    }
}
