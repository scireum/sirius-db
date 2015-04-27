/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing;

import com.google.common.collect.Maps;
import sirius.db.jdbc.Row;
import sirius.kernel.commons.Strings;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.mixing.annotations.Versioned;
import sirius.mixing.properties.AccessPath;
import sirius.mixing.properties.Mixing;
import sirius.mixing.properties.Property;
import sirius.mixing.schema.Column;
import sirius.mixing.schema.Table;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Created by aha on 29.11.14.
 */
public class EntityDescriptor {

    protected boolean versioned;
    protected Class<? extends Entity> type;
    protected String tableName;
    protected Map<String, Property> properties = Maps.newTreeMap();

    @Part(configPath = "mixing.namingSchema")
    private static NamingSchema namingSchema;

    public EntityDescriptor(Class<? extends Entity> type) {
        this.type = type;
        this.versioned = type.isAnnotationPresent(Versioned.class);
        this.tableName = namingSchema.generateTableName(type);
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isVersioned() {
        return versioned;
    }

    public Collection<Property> getProperties() {
        return properties.values();
    }

    public <E extends Entity> boolean isChanged(E entity, String name) {
        if (!entity.persistedData.containsKey(name)) {
            return true;
        }
        Property p = properties.get(name);
        if (p == null) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .withSystemErrorMessage("Unknown property '%s' for '%s'", name, type.getName())
                            .handle();
        }
        return !Objects.equals(entity.persistedData.get(name), p.getValue(entity));
    }

    public <E extends Entity> int getVersion(E entity) {
        return entity.version;
    }

    public void setVersion(Entity entity, int version) {
        entity.version = version;
    }


    public final void beforeSave(Entity entity) {
        beforeSaveChecks(entity);
        for (Property property : properties.values()) {
            property.onBeforeSave(entity);
        }
        onBeforeSave(entity);
    }

    protected void beforeSaveChecks(Entity entity) {

    }

    protected void onBeforeSave(Entity entity) {

    }

    public void afterSave(Entity entity) {
        for (Property property : properties.values()) {
            property.onAfterSave(entity);
        }
        onAfterSave(entity);
    }

    protected void onAfterSave(Entity entity) {

    }

    public void beforeDelete(Entity entity) {
        for (Property property : properties.values()) {
            property.onBeforeDelete(entity);
        }
        onBeforeDelete(entity);
    }

    protected void onBeforeDelete(Entity entity) {

    }

    public void afterDelete(Entity entity) {
        for (Property property : properties.values()) {
            property.onAfterDelete(entity);
        }
        onAfterDelete(entity);
    }

    protected void onAfterDelete(Entity entity) {

    }

    public Table createTable() {
        Table table = new Table();
        table.setName(tableName);

        Column idColumn = new Column();
        idColumn.setAutoIncrement(true);
        idColumn.setName("id");
        idColumn.setType(Types.BIGINT);
        idColumn.setLength(20);
        table.getColumns().add(idColumn);
        table.getPrimaryKey().add(idColumn.getName());

        if (isVersioned()) {
            Column versionColumn = new Column();
            versionColumn.setAutoIncrement(true);
            versionColumn.setName("version");
            versionColumn.setType(Types.BIGINT);
            versionColumn.setLength(20);
            table.getColumns().add(versionColumn);
        }

        for (Property p : properties.values()) {
            p.addColumns(table);
        }

        return table;
    }

    protected void initialize() {
        Mixing.addFields(AccessPath.IDENTITY, type, p -> {
            if (properties.containsKey(p.getName())) {
                OMA.LOG.SEVERE(Strings.apply(
                        "A property named '%s' already exists for the type '%s'. Skipping redefinition: %s",
                        p.getName(),
                        type.getSimpleName(),
                        p.getDefinition()));

            } else {
                properties.put(p.getName(), p);
            }
        });
    }

    public Entity readFrom(String alias, Row row) throws Exception {
        Entity entity = type.newInstance();
        entity.fetchRow = row;
        readIdAndVersion(alias, row, entity);
        for (Property p : getProperties()) {
            String columnName = (alias == null) ? p.getColumnName() : alias + "_" + p.getColumnName();
            if (row.hasValue(columnName)) {
                p.setValue(entity, row.getValue(columnName).get());
            }
        }
        return entity;
    }

    private void readIdAndVersion(String alias, Row row, Entity entity) {
        String idColumnLabel = getIdColumnLabel(alias).toUpperCase();
        if (!row.hasValue(idColumnLabel)) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .withSystemErrorMessage("Error loading entity from row: Missing id column for type '%s'.",
                                                    type.getName())
                            .handle();
        }
        entity.setId(row.getValue(idColumnLabel).asLong(-1));
        if (isVersioned()) {
            String versionColumnLabel = getVersionColumnLabel(alias).toUpperCase();
            if (!row.hasValue(versionColumnLabel)) {
                throw Exceptions.handle()
                                .to(OMA.LOG)
                                .withSystemErrorMessage(
                                        "Error loading entity from row: Missing version column for type '%s'.",
                                        type.getName())
                                .handle();
            }
            setVersion(entity, row.getValue(versionColumnLabel).asInt(0));
        }
    }

    public Entity readFrom(String alias, Set<String> columns, ResultSet rs) throws Exception {
        Entity entity = type.newInstance();
        readIdAndVersion(alias, columns, rs, entity);
        for (Property p : getProperties()) {
            String columnName = (alias == null) ? p.getColumnName() : alias + "_" + p.getColumnName();
            if (columns.contains(columnName.toUpperCase())) {
                p.setValue(entity, rs.getObject(columnName));
            }
        }
        return entity;
    }

    private void readIdAndVersion(String alias, Set<String> columns, ResultSet rs, Entity entity) throws SQLException {
        String idColumnLabel = getIdColumnLabel(alias);
        if (!columns.contains(idColumnLabel.toUpperCase())) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .withSystemErrorMessage("Error loading entity from row: Missing id column for type '%s'.",
                                                    type.getName())
                            .handle();
        }
        entity.setId(rs.getLong(idColumnLabel));
        if (isVersioned()) {
            String versionColumnLabel = getVersionColumnLabel(alias);
            if (!columns.contains(versionColumnLabel.toUpperCase())) {
                throw Exceptions.handle()
                                .to(OMA.LOG)
                                .withSystemErrorMessage(
                                        "Error loading entity from row: Missing version column for type '%s'.",
                                        type.getName())
                                .handle();
            }
            setVersion(entity, rs.getInt(versionColumnLabel));
        }
    }

    private String getVersionColumnLabel(String alias) {
        return (alias == null) ? "version" : alias + "_version";
    }

    private String getIdColumnLabel(String alias) {
        return (alias == null) ? "id" : alias + "_id";
    }

    @Override
    public String toString() {
        return tableName + " (" + type.getName() + ")";
    }
}
