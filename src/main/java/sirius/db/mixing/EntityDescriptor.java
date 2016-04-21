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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import sirius.db.jdbc.Row;
import sirius.db.mixing.annotations.AfterDelete;
import sirius.db.mixing.annotations.AfterSave;
import sirius.db.mixing.annotations.BeforeDelete;
import sirius.db.mixing.annotations.BeforeSave;
import sirius.db.mixing.annotations.Mixin;
import sirius.db.mixing.annotations.Transient;
import sirius.db.mixing.annotations.Versioned;
import sirius.db.mixing.schema.Table;
import sirius.db.mixing.schema.TableColumn;
import sirius.kernel.Sirius;
import sirius.kernel.commons.MultiMap;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.Injector;
import sirius.kernel.di.PartCollection;
import sirius.kernel.di.std.Parts;
import sirius.kernel.health.Exceptions;
import sirius.kernel.nls.NLS;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Represents the recipe on how to write an entity class into a database table.
 * <p>
 * For each subclass of {@link Entity} a <tt>descriptor</tt> is created by the {@link Schema}. This descriptor
 * is in charge of reading and writing from and to the database as well as ensuring consistency of the data.
 * <p>
 * The descriptor is automatically creates and its properties are discovered by checking all fields, composites
 * and mixins.
 */
public class EntityDescriptor {

    /**
     * Determines if the entity uses optimistic locking
     */
    protected boolean versioned;

    /**
     * Contains the entity class
     */
    protected Class<? extends Entity> type;

    /**
     * Contains the instance which was created by the {@link EntityLoadAction}
     */
    protected final Entity referenceInstance;

    /**
     * Contains the effective table name in the database
     */
    protected String tableName;

    /**
     * Contains all properties (defined via fields, composites or mixins)
     */
    protected Map<String, Property> properties = Maps.newTreeMap();

    /**
     * A list of all additional handlers to be executed once an entity was deleted
     */
    protected List<Consumer<Entity>> cascadeDeleteHandlers = Lists.newArrayList();

    /**
     * A list of all additional handlers to be executed once an entity is about to be deleted
     */
    protected List<Consumer<Entity>> beforeDeleteHandlers = Lists.newArrayList();

    /**
     * A list of all additional handlers to be executed once an entity was successfully deleted
     */
    protected List<Consumer<Entity>> afterDeleteHandlers = Lists.newArrayList();

    /**
     * A list of all additional handlers to be executed once an entity is about to be saved
     */
    protected List<Consumer<Entity>> beforeSaveHandlers = Lists.newArrayList();

    /**
     * A list of all additional handlers to be executed once an entity is was saved
     */
    protected List<Consumer<Entity>> afterSaveHandlers = Lists.newArrayList();

    protected Config legacyInfo;
    protected Map<String, String> columnAliases;

    /**
     * Creates a new entity for the given reference instance.
     *
     * @param referenceInstance the instance from which the descriptor is filled
     */
    protected EntityDescriptor(Entity referenceInstance) {
        this.type = referenceInstance.getClass();
        this.referenceInstance = referenceInstance;
        this.versioned = type.isAnnotationPresent(Versioned.class);
        this.tableName = type.getSimpleName().toLowerCase();
        String configKey = "mixing.legacy." + type.getSimpleName();
        this.legacyInfo = Sirius.getConfig().hasPath(configKey) ? Sirius.getConfig().getConfig(configKey) : null;
        if (legacyInfo != null) {
            if (legacyInfo.hasPath("tableName")) {
                this.tableName = legacyInfo.getString("tableName");
            }
            if (legacyInfo.hasPath("alias")) {
                Config aliases = legacyInfo.getConfig("alias");
                columnAliases = Maps.newHashMap();
                for (Map.Entry<String, ConfigValue> entry : aliases.entrySet()) {
                    columnAliases.put(entry.getKey(), Value.of(entry.getValue().unwrapped()).asString());
                }
            }
        }
    }

    /**
     * Returns the "end user friendly" name of the entity.
     * <p>
     * This is determined by calling <tt>NLS.get()</tt>
     * with the full class name or as fallback the simple class name as lower case, prepended with "Model.". Therefore
     * the property keys for "org.acme.model.Customer" would be the class name and "Model.customer". The fallback
     * key will be the same which is tried for a property named "customer" and can therefore be reused.
     *
     * @return a translated name which can be shown to the end user
     */
    public String getLabel() {
        return NLS.getIfExists(getType().getName(), NLS.getCurrentLang())
                  .orElseGet(() -> NLS.get("Model." + type.getSimpleName().toLowerCase()));
    }

    /**
     * Returns the "end user friendly" plural of the entity.
     * <p>
     * The i18n keys tried are the same as for {@link #getLabel()} with ".plural" appended.
     *
     * @return a translated plural which can be shown to the end user
     */
    public String getPluralLabel() {
        return NLS.get(getType().getName() + ".plural");
    }

    /**
     * Returns the effective table name.
     *
     * @return the name of the table in the RDBMS
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Determiens if optimistic locking via version numbers is supported.
     *
     * @return <tt>true</tt> if versioning is supported, <tt>false</tt> otherwise
     */
    public boolean isVersioned() {
        return versioned;
    }

    /*
     * Links all properties to setup foreign keys
     */
    protected void link() {
        for (Property p : properties.values()) {
            p.link();
        }
    }

    public Collection<Property> getProperties() {
        return properties.values();
    }

    public <E extends Entity> boolean isFetched(E entity, Property property) {
        if (entity.isNew()) {
            return false;
        }
        return entity.persistedData.containsKey(property);
    }

    public <E extends Entity> boolean isChanged(E entity, Property property) {
        return !Objects.equals(entity.persistedData.get(property), property.getValue(entity));
    }

    public <E extends Entity> int getVersion(E entity) {
        return entity.getVersion();
    }

    public void setVersion(Entity entity, int version) {
        entity.version = version;
    }

    protected final void beforeSave(Entity entity) {
        beforeSaveChecks(entity);
        for (Consumer<Entity> c : beforeSaveHandlers) {
            c.accept(entity);
        }
        for (Property property : properties.values()) {
            property.onBeforeSave(entity);
        }
        onBeforeSave(entity);
    }

    protected void beforeSaveChecks(Entity entity) {

    }

    protected void onBeforeSave(Entity entity) {

    }

    protected void afterSave(Entity entity) {
        for (Consumer<Entity> c : afterSaveHandlers) {
            c.accept(entity);
        }
        for (Property property : properties.values()) {
            property.onAfterSave(entity);
        }
        onAfterSave(entity);

        // Reset persisted data
        entity.persistedData.clear();
        for (Property p : getProperties()) {
            entity.persistedData.put(p, p.getValue(entity));
        }
    }

    protected void onAfterSave(Entity entity) {

    }

    protected void beforeDelete(Entity entity) {
        for (Property property : properties.values()) {
            property.onBeforeDelete(entity);
        }
        onBeforeDelete(entity);
        for (Consumer<Entity> handler : beforeDeleteHandlers) {
            handler.accept(entity);
        }
        for (Consumer<Entity> handler : cascadeDeleteHandlers) {
            handler.accept(entity);
        }
    }

    public void addCascadeDeleteHandler(Consumer<Entity> handler) {
        cascadeDeleteHandlers.add(handler);
    }

    public void addBeforeDeleteHandler(Consumer<Entity> handler) {
        beforeDeleteHandlers.add(handler);
    }

    protected void onBeforeDelete(Entity entity) {

    }

    protected void afterDelete(Entity entity) {
        for (Property property : properties.values()) {
            property.onAfterDelete(entity);
        }
        for (Consumer<Entity> handler : afterDeleteHandlers) {
            handler.accept(entity);
        }
        onAfterDelete(entity);
    }

    protected void onAfterDelete(Entity entity) {

    }

    protected Table createTable() {
        Table table = new Table();
        table.setName(tableName);

        TableColumn idColumn = new TableColumn();
        idColumn.setAutoIncrement(true);
        idColumn.setName(Entity.ID.getName());
        idColumn.setType(Types.BIGINT);
        idColumn.setLength(20);
        table.getColumns().add(idColumn);
        table.getPrimaryKey().add(idColumn.getName());

        if (isVersioned()) {
            TableColumn versionColumn = new TableColumn();
            versionColumn.setAutoIncrement(true);
            versionColumn.setName(Entity.VERSION.getName());
            versionColumn.setType(Types.BIGINT);
            versionColumn.setLength(20);
            table.getColumns().add(versionColumn);
        }

        for (Property p : properties.values()) {
            p.contributeToTable(table);
        }

        if (legacyInfo != null && legacyInfo.hasPath("rename")) {
            Config renamedColumns = legacyInfo.getConfig("rename");
            for (TableColumn col : table.getColumns()) {
                if (columnAliases != null) {
                    String alias = columnAliases.get(col.getName());
                    if (Strings.isFilled(alias)) {
                        col.setName(alias);
                    }
                }
                if (renamedColumns != null && renamedColumns.hasPath(col.getName())) {
                    col.setOldName(renamedColumns.getString(col.getName()));
                }
            }
        }

        return table;
    }

    protected void initialize() {
        addFields(this, AccessPath.IDENTITY, type, p -> {
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

    /*
     * Contains all known property factories. These are used to transform fields defined by entity classes to
     * properties
     */
    @Parts(PropertyFactory.class)
    protected static PartCollection<PropertyFactory> factories;

    @Parts(PropertyModifier.class)
    protected static PartCollection<PropertyModifier> modifiers;

    private static MultiMap<Class<? extends Mixable>, Class<?>> mixins;

    @SuppressWarnings("unchecked")
    private static Collection<Class<?>> getMixins(Class<? extends Mixable> forClass) {
        if (mixins == null) {
            MultiMap<Class<? extends Mixable>, Class<?>> mixinMap = MultiMap.create();
            for (Class<?> mixinClass : Injector.context().getParts(Mixin.class, Class.class)) {
                Class<?> target = mixinClass.getAnnotation(Mixin.class).value();
                if (Mixable.class.isAssignableFrom(target)) {
                    mixinMap.put((Class<? extends Mixable>) target, mixinClass);
                } else {
                    OMA.LOG.WARN("Mixing class '%s' has a non mixable target class (%s). Skipping mixin.",
                                 mixinClass.getName(),
                                 target.getName());
                }
            }
            mixins = mixinMap;
        }

        return mixins.get(forClass);
    }

    /*
     * Adds all properties of the given class (and its superclasses)
     */
    @SuppressWarnings("unchecked")
    public static void addFields(EntityDescriptor descriptor,
                                 AccessPath accessPath,
                                 Class<?> clazz,
                                 Consumer<Property> propertyConsumer) {
        addFields(descriptor, accessPath, clazz, clazz, propertyConsumer);
    }

    /*
     * Adds all properties of the given class (and its superclasses)
     */
    @SuppressWarnings("unchecked")
    private static void addFields(EntityDescriptor descriptor,
                                  AccessPath accessPath,
                                  Class<?> rootClass,
                                  Class<?> clazz,
                                  Consumer<Property> propertyConsumer) {
        for (Field field : clazz.getDeclaredFields()) {
            addField(descriptor, accessPath, rootClass, clazz, field, propertyConsumer);
        }
        for (Method m : clazz.getDeclaredMethods()) {
            if (m.isAnnotationPresent(BeforeSave.class)) {
                if (!Modifier.isProtected(m.getModifiers())) {
                    OMA.LOG.WARN("BeforeSave handler %s.%s is not declared protected!",
                                 m.getDeclaringClass().getName(),
                                 m.getName());
                }
                descriptor.beforeSaveHandlers.add(e -> {
                    invokeHandler(accessPath, m, e);
                });
            } else if (m.isAnnotationPresent(AfterSave.class)) {
                if (!Modifier.isProtected(m.getModifiers())) {
                    OMA.LOG.WARN("AfterSave handler %s.%s is not declared protected!",
                                 m.getDeclaringClass().getName(),
                                 m.getName());
                }
                descriptor.afterSaveHandlers.add(e -> {
                    invokeHandler(accessPath, m, e);
                });
            } else if (m.isAnnotationPresent(BeforeDelete.class)) {
                if (!Modifier.isProtected(m.getModifiers())) {
                    OMA.LOG.WARN("BeforeDelete handler %s.%s is not declared protected!",
                                 m.getDeclaringClass().getName(),
                                 m.getName());
                }
                descriptor.beforeDeleteHandlers.add(e -> {
                    invokeHandler(accessPath, m, e);
                });
            } else if (m.isAnnotationPresent(AfterDelete.class)) {
                if (!Modifier.isProtected(m.getModifiers())) {
                    OMA.LOG.WARN("AfterDelete handler %s.%s is not declared protected!",
                                 m.getDeclaringClass().getName(),
                                 m.getName());
                }
                descriptor.afterDeleteHandlers.add(e -> {
                    invokeHandler(accessPath, m, e);
                });
            }
        }

        if (Mixable.class.isAssignableFrom(clazz)) {
            for (Class<?> mixin : getMixins((Class<? extends Mixable>) clazz)) {
                addFields(descriptor, expandAccessPath(mixin, accessPath), rootClass, mixin, propertyConsumer);
            }
        }

        if (clazz.getSuperclass() != null && !Object.class.equals(clazz.getSuperclass())) {
            addFields(descriptor, accessPath, rootClass, clazz.getSuperclass(), propertyConsumer);
        }
    }

    private static void invokeHandler(AccessPath accessPath, Method m, Entity e) {
        try {
            m.setAccessible(true);
            if (m.getParameterCount() == 0) {
                m.invoke(accessPath.apply(e));
            } else {
                m.invoke(accessPath.apply(e), e);
            }
        } catch (IllegalAccessException ex) {
            throw Exceptions.handle(OMA.LOG, ex);
        } catch (InvocationTargetException ex) {
            throw Exceptions.handle(OMA.LOG, ex.getTargetException());
        }
    }

    private static AccessPath expandAccessPath(Class<?> mixin, AccessPath accessPath) {
        return accessPath.append(mixin.getSimpleName() + Column.SUBFIELD_SEPARATOR, obj -> ((Mixable) obj).as(mixin));
    }

    private static void addField(EntityDescriptor descriptor,
                                 AccessPath accessPath,
                                 Class<?> rootClass,
                                 Class<?> clazz,
                                 Field field,
                                 Consumer<Property> propertyConsumer) {
        if (!field.isAnnotationPresent(Transient.class) && !Modifier.isStatic(field.getModifiers())) {
            for (PropertyFactory f : factories.getParts()) {
                if (f.accepts(field)) {
                    f.create(descriptor, accessPath, field, p -> propertyConsumer.accept(modifyProperty(p)));
                    return;
                }
            }
            OMA.LOG.WARN("Cannot create property %s in type %s (%s)",
                         field.getName(),
                         rootClass.getName(),
                         clazz.getName());
        }
    }

    private static Property modifyProperty(Property p) {
        for (PropertyModifier modifier : modifiers) {
            if (modifier.targetType() == null || modifier.targetType()
                                                         .isAssignableFrom(p.getField().getDeclaringClass())) {
                if (Strings.isEmpty(modifier.targetFieldName()) || Strings.areEqual(p.getField().getName(),
                                                                                    modifier.targetFieldName())) {
                    p = modifier.modify(p);
                }
            }
        }

        return p;
    }

    public Entity readFrom(String alias, Row row) throws Exception {
        Entity entity = type.newInstance();
        entity.fetchRow = row;
        readIdAndVersion(alias, row, entity);
        for (Property p : getProperties()) {
            String columnName = (alias == null) ? p.getColumnName() : alias + "_" + p.getColumnName();
            if (row.hasValue(columnName)) {
                p.setValueFromColumn(entity, row.getValue(columnName).get());
                entity.persistedData.put(p, p.getValue(entity));
            }
        }
        return entity;
    }

    private void readIdAndVersion(String alias, Row row, Entity entity) {
        String idColumnLabel = getIdColumnLabel(alias).toUpperCase();
        if (row.hasValue(idColumnLabel)) {
            entity.setId(row.getValue(idColumnLabel).asLong(-1));
        }
        if (isVersioned()) {
            String versionColumnLabel = getVersionColumnLabel(alias).toUpperCase();
            if (row.hasValue(versionColumnLabel)) {
                setVersion(entity, row.getValue(versionColumnLabel).asInt(0));
            }
        }
    }

    protected Entity readFrom(String alias, Set<String> columns, ResultSet rs) throws Exception {
        Entity entity = type.newInstance();
        readIdAndVersion(alias, columns, rs, entity);
        for (Property p : getProperties()) {
            String columnName = (alias == null) ? p.getColumnName() : alias + "_" + p.getColumnName();
            if (columns.contains(columnName.toUpperCase())) {
                p.setValueFromColumn(entity, rs.getObject(columnName));
                entity.persistedData.put(p, p.getValue(entity));
            }
        }
        return entity;
    }

    private void readIdAndVersion(String alias, Set<String> columns, ResultSet rs, Entity entity) throws SQLException {
        String idColumnLabel = getIdColumnLabel(alias);
        if (columns.contains(idColumnLabel.toUpperCase())) {
            entity.setId(rs.getLong(idColumnLabel));
        }
        if (isVersioned()) {
            String versionColumnLabel = getVersionColumnLabel(alias);
            if (columns.contains(versionColumnLabel.toUpperCase())) {
                setVersion(entity, rs.getInt(versionColumnLabel));
            }
        }
    }

    private String getVersionColumnLabel(String alias) {
        return (alias == null) ? "version" : alias + "_version";
    }

    private String getIdColumnLabel(String alias) {
        return (alias == null) ? "id" : alias + "_id";
    }

    public String rewriteColumnName(String basicColumnName) {
        if (columnAliases != null && columnAliases.containsKey(basicColumnName)) {
            return columnAliases.get(basicColumnName);
        }
        return basicColumnName;
    }

    @Override
    public String toString() {
        return tableName + " [" + type.getName() + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof EntityDescriptor)) {
            return false;
        }

        return type.equals(((EntityDescriptor) obj).type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }

    public Class<? extends Entity> getType() {
        return type;
    }

    public Property getProperty(Column column) {
        if (column.getParent() != null) {
            throw new IllegalArgumentException(Strings.apply("Cannot fetch joined property: %s", column));
        }
        return getProperty(column.getName());
    }

    public Property getProperty(String property) {
        Property prop = properties.get(property.replace('.', '_'));
        if (prop == null) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .withSystemErrorMessage("Cannot find property '%s' for type '%s'.",
                                                    property,
                                                    type.getName())
                            .handle();
        }

        return prop;
    }

    public Entity getReferenceInstance() {
        return referenceInstance;
    }
}
