/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import sirius.db.mixing.annotations.AfterDelete;
import sirius.db.mixing.annotations.AfterSave;
import sirius.db.mixing.annotations.BeforeDelete;
import sirius.db.mixing.annotations.BeforeSave;
import sirius.db.mixing.annotations.Mixin;
import sirius.db.mixing.annotations.OnValidate;
import sirius.db.mixing.annotations.Realm;
import sirius.db.mixing.annotations.RelationName;
import sirius.db.mixing.annotations.Transient;
import sirius.db.mixing.annotations.Versioned;
import sirius.kernel.Sirius;
import sirius.kernel.commons.MultiMap;
import sirius.kernel.commons.PriorityCollector;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.commons.ValueHolder;
import sirius.kernel.commons.ValueSupplier;
import sirius.kernel.di.Injector;
import sirius.kernel.di.std.PriorityParts;
import sirius.kernel.health.Exceptions;
import sirius.kernel.nls.NLS;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Used by {@link Mixing} to describe all properties and consistency checks of a managed entity.
 * <p>
 * These entities and be stored and loaded from various databases using an appropriate {@link BaseMapper mapper}.
 */
public class EntityDescriptor {

    /**
     * Contains the effective / technical to use in the datasource
     */
    private String relationName;

    /**
     * Contains the realm of the mapped entity;
     */
    private final String realm;

    /**
     * Contains the entity class
     */
    protected final Class<?> type;

    /**
     * Contains a reference instance required by runtime inspections.
     */
    private Object referenceInstance;

    /**
     * Contains all properties (defined via fields, composites or mixins)
     */
    protected final Map<String, Property> properties = new TreeMap<>();

    /**
     * Contains a set of all composites contained within this entity.
     */
    protected final Set<Class<? extends Composite>> composites = new HashSet<>();

    /**
     * Contains a set of all mixins available for this entity.
     */
    protected final Set<Class<? extends Mixable>> mixins = new HashSet<>();

    /**
     * A list of all additional handlers to be executed once an entity was deleted
     */
    protected final List<Consumer<Object>> cascadeDeleteHandlers = new ArrayList<>();

    /**
     * A list of all additional handlers to be executed once an entity is about to be deleted
     */
    protected final List<Consumer<Object>> beforeDeleteHandlers = new ArrayList<>();

    /**
     * A list of all additional handlers to be executed once an entity was successfully deleted
     */
    protected final List<Consumer<Object>> afterDeleteHandlers = new ArrayList<>();

    /**
     * A list of all additional handlers to be executed once an entity is about to be saved
     */
    protected List<Consumer<Object>> sortedBeforeSaveHandlers;

    /**
     * Collects handlers which are executed before entity is saved, need to be in order (some checks might depend on others),
     * {@link BeforeSave} permits to specify a property which is used to here to sort the handlers.
     *
     * <tt>sortedBeforeSaveHandlers</tt> will be filled when first accessed and provide a properly sorted list
     */
    protected PriorityCollector<Consumer<Object>> beforeSaveHandlerCollector = PriorityCollector.create();

    /**
     * A list of all additional handlers to be executed once an entity is was saved
     */
    protected final List<Consumer<Object>> afterSaveHandlers = new ArrayList<>();

    /**
     * A list of all handlers to be executed once an entity is validated
     */
    protected final List<BiConsumer<Object, Consumer<String>>> validateHandlers = new ArrayList<>();

    /**
     * Contains all known property factories. These are used to transform fields defined by entity classes to
     * properties
     */
    @PriorityParts(PropertyFactory.class)
    protected static List<PropertyFactory> factories;

    /**
     * Contains all mixins known to the system
     */
    private static MultiMap<Class<? extends Mixable>, Class<?>> allMixins;

    protected Config legacyInfo;
    protected Map<String, String> columnAliases;
    protected boolean versioned;
    protected BaseMapper<?, ?, ?> mapper;

    /**
     * Creates a new entity for the given reference instance.
     *
     * @param type the type from which the descriptor is filled
     */
    protected EntityDescriptor(Class<?> type) {
        this.type = type;
        RelationName relationNameAnnotation = type.getAnnotation(RelationName.class);
        this.relationName =
                relationNameAnnotation != null ? relationNameAnnotation.value() : type.getSimpleName().toLowerCase();
        Realm realmAnnotation = type.getAnnotation(Realm.class);
        this.realm = realmAnnotation != null ? realmAnnotation.value() : Mixing.DEFAULT_REALM;
        this.versioned = type.isAnnotationPresent(Versioned.class);

        try {
            this.referenceInstance = type.newInstance();
        } catch (Exception e) {
            Exceptions.handle()
                      .to(Mixing.LOG)
                      .error(e)
                      .withSystemErrorMessage("Cannot create reference instance of mapped type: %s - %s (%s)",
                                              type.getName())
                      .handle();
        }

        loadLegacyInfo(type);
    }

    private void loadLegacyInfo(Class<?> type) {
        String configKey = "mixing.legacy." + type.getSimpleName();
        this.legacyInfo = Sirius.getSettings().getConfig().hasPath(configKey) ?
                          Sirius.getSettings().getConfig().getConfig(configKey) :
                          null;
        if (legacyInfo != null) {
            if (legacyInfo.hasPath("tableName")) {
                this.relationName = legacyInfo.getString("tableName");
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
     * Returns the technical simple name to use.
     * <p>
     * This is e.g. used to determine table names or collection names in the datasource.
     *
     * @return the technical name to use.
     */
    public String getRelationName() {
        return relationName;
    }

    /**
     * Returns the realm of entities of this type.
     * <p>
     * Depending on the mapper and database, this can control the effective target database or storage settings used
     * for this entity.
     *
     * @return the realm of this type
     */
    public String getRealm() {
        return realm;
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
        return NLS.get(getType().getSimpleName() + ".plural");
    }

    /**
     * Links all properties to setup foreign keys and delete constraints.
     */
    protected void link() {
        for (Property p : properties.values()) {
            p.link();
        }
    }

    /**
     * Returns all properties known by this descriptor.
     * <p>
     * These are either properties declared by the type (class) of the entity and its superclasses or once defined
     * by {@link Composite}s and {@link Mixin}s.
     *
     * @return a list of all properties of the entity
     */
    public Collection<Property> getProperties() {
        return properties.values();
    }

    /**
     * Determines if a value was fetched in the given entity for the given property.
     * <p>
     * As queries might select only certain fields, not all properties are filled for an entity.
     *
     * @param entity   the entity to check
     * @param property the property to check for
     * @return <tt>true</tt> if a value was fetched from the database, <tt>false</tt> otherwise
     */
    public boolean isFetched(BaseEntity<?> entity, Property property) {
        if (entity.isNew()) {
            return false;
        }
        return entity.persistedData.containsKey(property);
    }

    /**
     * Determines if the value for the property was changed since it was last fetched from the database.
     *
     * @param entity   the entity to check
     * @param property the property to check for
     * @return <tt>true</tt> if the value was changed, <tt>false</tt> otherwise
     */
    public boolean isChanged(BaseEntity<?> entity, Property property) {
        return !Objects.equals(entity.persistedData.get(property), property.getValue(entity));
    }

    /**
     * Executes all <tt>beforedSaveHandlers</tt> known to the descriptor.
     *
     * @param entity the entity to perform the handlers on
     */
    public final void beforeSave(Object entity) {
        for (Consumer<Object> c : getSortedBeforeSaveHandlers()) {
            c.accept(entity);
        }
        for (Property property : properties.values()) {
            property.onBeforeSave(entity);
        }
    }

    private List<Consumer<Object>> getSortedBeforeSaveHandlers() {
        if (sortedBeforeSaveHandlers == null) {
            sortedBeforeSaveHandlers = beforeSaveHandlerCollector.getData();
            beforeSaveHandlerCollector = null;
        }

        return sortedBeforeSaveHandlers;
    }

    /**
     * Executes all <tt>afterSaveHandlers</tt> once an entity was saved to the database.
     *
     * @param entity the entity which was saved
     */
    public void afterSave(Object entity) {
        for (Consumer<Object> c : afterSaveHandlers) {
            c.accept(entity);
        }
        for (Property property : properties.values()) {
            property.onAfterSave(entity);
        }

        if (isBaseEntity(entity)) {
            // Reset persisted data
            asBaseEntity(entity).persistedData.clear();
            for (Property p : getProperties()) {
                asBaseEntity(entity).persistedData.put(p, p.getValueAsCopy(entity));
            }
        }
    }

    private BaseEntity<?> asBaseEntity(Object entity) {
        return (BaseEntity<?>) entity;
    }

    private boolean isBaseEntity(Object entity) {
        return entity instanceof BaseEntity;
    }

    /**
     * Executes all validation handlers on the given entity.
     *
     * @param entity the entity to validate
     * @return a list of all validation warnings
     */
    public List<String> validate(Object entity) {
        List<String> warnings = new ArrayList<>();
        for (BiConsumer<Object, Consumer<String>> validator : validateHandlers) {
            validator.accept(entity, warnings::add);
        }

        return warnings;
    }

    /**
     * Determines if the given entity has validation warnings.
     *
     * @param entity the entity to check
     * @return <tt>true</tt> if there are validation warnings, <tt>false</tt> otherwise
     */
    public boolean hasValidationWarnings(Object entity) {
        for (BiConsumer<Object, Consumer<String>> validator : validateHandlers) {
            ValueHolder<Boolean> hasWarnings = ValueHolder.of(false);
            validator.accept(entity, warning -> hasWarnings.accept(true));
            if (hasWarnings.get()) {
                return true;
            }
        }

        return false;
    }

    /**
     * Invokes all <tt>beforeDeleteHandlers</tt> and then all <tt>cascadeDeleteHandlers</tt> for the given entity.
     *
     * @param entity the entity which is about to be deleted
     */
    public void beforeDelete(Object entity) {
        for (Property property : properties.values()) {
            property.onBeforeDelete(entity);
        }
        for (Consumer<Object> handler : beforeDeleteHandlers) {
            handler.accept(entity);
        }
        for (Consumer<Object> handler : cascadeDeleteHandlers) {
            handler.accept(entity);
        }
    }

    /**
     * Adds a cascade handler for entities managed by this descriptor.
     *
     * @param handler the handler to add
     */
    public void addCascadeDeleteHandler(Consumer<Object> handler) {
        cascadeDeleteHandlers.add(handler);
    }

    /**
     * Adds a before delete handler for entities managed by this descriptor.
     *
     * @param handler the handler to add
     */
    public void addBeforeDeleteHandler(Consumer<Object> handler) {
        beforeDeleteHandlers.add(handler);
    }

    /**
     * Adds a validation handler
     *
     * @param handler the handler to add
     */
    public void addValidationHandler(BiConsumer<Object, Consumer<String>> handler) {
        validateHandlers.add(handler);
    }

    /**
     * Invokes all <tt>afterDeleteHandlers</tt>.
     *
     * @param entity the entity which was deleted
     */
    public void afterDelete(BaseEntity<?> entity) {
        for (Property property : properties.values()) {
            property.onAfterDelete(entity);
        }
        for (Consumer<Object> handler : afterDeleteHandlers) {
            handler.accept(entity);
        }
    }

    /**
     * Loads all properties from the fields being present in the target type.
     */
    protected void initialize() {
        addFields(this, AccessPath.IDENTITY, type, p -> {
            if (properties.containsKey(p.getName())) {
                Mixing.LOG.SEVERE(Strings.apply(
                        "A property named '%s' already exists for the type '%s'. Skipping redefinition: %s",
                        p.getName(),
                        type.getSimpleName(),
                        p.getDefinition()));
            } else {
                properties.put(p.getName(), p);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static Collection<Class<?>> getMixins(Class<? extends Mixable> forClass) {
        if (allMixins == null) {
            MultiMap<Class<? extends Mixable>, Class<?>> mixinMap = MultiMap.create();
            for (Class<?> mixinClass : Injector.context().getParts(Mixin.class, Class.class)) {
                Class<?> target = mixinClass.getAnnotation(Mixin.class).value();
                if (Mixable.class.isAssignableFrom(target)) {
                    mixinMap.put((Class<? extends Mixable>) target, mixinClass);
                } else {
                    Mixing.LOG.WARN("Mixing class '%s' has a non mixable target class (%s). Skipping mixin.",
                                    mixinClass.getName(),
                                    target.getName());
                }
            }
            allMixins = mixinMap;
        }

        return allMixins.get(forClass);
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
            processMethod(descriptor, accessPath, m);
        }

        if (Mixable.class.isAssignableFrom(clazz)) {
            for (Class<?> mixin : getMixins((Class<? extends Mixable>) clazz)) {
                addFields(descriptor, expandAccessPath(mixin, accessPath), rootClass, mixin, propertyConsumer);
                descriptor.mixins.add((Class<? extends Mixable>) mixin);
            }
        }

        if (clazz.getSuperclass() != null
            && !Mixable.class.equals(clazz.getSuperclass())
            && !Object.class.equals(clazz.getSuperclass())) {
            addFields(descriptor, accessPath, rootClass, clazz.getSuperclass(), propertyConsumer);
        }
    }

    private static void processMethod(EntityDescriptor descriptor, AccessPath accessPath, Method method) {
        if (method.isAnnotationPresent(BeforeSave.class)) {
            handleBeforeSaveMethod(descriptor, accessPath, method);
        }
        if (method.isAnnotationPresent(AfterSave.class)) {
            warnOnWrongVisibility(method);
            descriptor.afterSaveHandlers.add(e -> invokeHandler(accessPath, method, e));
        }
        if (method.isAnnotationPresent(BeforeDelete.class)) {
            warnOnWrongVisibility(method);
            descriptor.beforeDeleteHandlers.add(e -> invokeHandler(accessPath, method, e));
        }
        if (method.isAnnotationPresent(AfterDelete.class)) {
            warnOnWrongVisibility(method);
            descriptor.afterDeleteHandlers.add(e -> invokeHandler(accessPath, method, e));
        }
        if (method.isAnnotationPresent(OnValidate.class)) {
            handleOnValidateMethod(descriptor, accessPath, method);
        }
    }

    private static void handleOnValidateMethod(EntityDescriptor descriptor, AccessPath accessPath, Method method) {
        warnOnWrongVisibility(method);
        if (method.getParameterCount() == 1 && method.getParameterTypes()[0] == Consumer.class) {
            // When declared within an entity, we only have Consumer<String> as parameter
            descriptor.validateHandlers.add((e, c) -> invokeHandler(accessPath, method, e, c));
        } else if (method.getParameterCount() == 2 && method.getParameterTypes()[1] == Consumer.class) {
            // When declared within a mixin, we have the entity itself as first parameter
            // and the consumer as second...
            descriptor.validateHandlers.add((e, c) -> invokeHandler(accessPath, method, e, e, c));
        } else {
            Mixing.LOG.WARN("OnValidate handler %s.%s doesn't have Consumer<String> as parameter!",
                            method.getDeclaringClass().getName(),
                            method.getName());
        }
    }

    private static void handleBeforeSaveMethod(EntityDescriptor descriptor, AccessPath accessPath, Method method) {
        warnOnWrongVisibility(method);
        if (descriptor.beforeSaveHandlerCollector == null) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .withSystemErrorMessage("Cannot provide a before-save-handler, as the sorted list was"
                                                    + " already computed. Descriptor: %s, Method: %s.%s",
                                                    descriptor.getType().getName(),
                                                    method.getDeclaringClass().getName(),
                                                    method.getName())
                            .handle();
        }
        descriptor.beforeSaveHandlerCollector.add(method.getAnnotation(BeforeSave.class).priority(),
                                                  e -> invokeHandler(accessPath, method, e));
    }

    private static void warnOnWrongVisibility(Method method) {
        if (!Modifier.isProtected(method.getModifiers())) {
            Mixing.LOG.WARN("Handler %s.%s is not declared protected!",
                            method.getDeclaringClass().getName(),
                            method.getName());
        }
    }

    private static void invokeHandler(AccessPath accessPath, Method m, Object entity, Object... params) {
        try {
            m.setAccessible(true);
            if (m.getParameterCount() == 0) {
                m.invoke(accessPath.apply(entity));
            } else {
                m.invoke(accessPath.apply(entity), params.length == 0 ? new Object[]{entity} : params);
            }
        } catch (IllegalAccessException ex) {
            throw Exceptions.handle(Mixing.LOG, ex);
        } catch (InvocationTargetException ex) {
            Exceptions.ignore(ex);
            throw Exceptions.handle(Mixing.LOG, ex.getTargetException());
        }
    }

    private static AccessPath expandAccessPath(Class<?> mixin, AccessPath accessPath) {
        return accessPath.append(mixin.getSimpleName() + Mapping.SUBFIELD_SEPARATOR, obj -> ((Mixable) obj).as(mixin));
    }

    private static void addField(EntityDescriptor descriptor,
                                 AccessPath accessPath,
                                 Class<?> rootClass,
                                 Class<?> clazz,
                                 Field field,
                                 Consumer<Property> propertyConsumer) {
        if (!field.isAnnotationPresent(Transient.class) && !Modifier.isStatic(field.getModifiers())) {
            for (PropertyFactory f : factories) {
                if (f.accepts(descriptor, field)) {
                    f.create(descriptor, accessPath, field, propertyConsumer);
                    return;
                }
            }
            Mixing.LOG.WARN("Cannot create property %s in type %s (%s)",
                            field.getName(),
                            rootClass.getName(),
                            clazz.getName());
        }
    }

    /**
     * Creates an entity from the given result row.
     *
     * @param mapperType the mapper which is currently active
     * @param alias      the field alias used to generate unique column names
     * @param supplier   used to provide values for a given column name
     * @return an entity containing the values of the given result row
     * @throws Exception in case of an error while building the entity
     */
    public Object make(Class<? extends BaseMapper<?, ?, ?>> mapperType, String alias, ValueSupplier<String> supplier)
            throws Exception {
        Object entity = type.newInstance();

        for (Property p : getProperties()) {
            String columnName = (alias == null) ? p.getPropertyName() : alias + "_" + p.getPropertyName();
            Value data = supplier.apply(columnName);
            if (data != null) {
                p.setValueFromDatasource(mapperType, entity, data);
                if (isBaseEntity(entity)) {
                    asBaseEntity(entity).persistedData.put(p, p.getValueAsCopy(entity));
                }
            }
        }

        return entity;
    }

    /**
     * Applies legacy renaming rules to determine the effective property name based on the name generated by the
     * property.
     *
     * @param basicPropertyName the generic property name generated by the property
     * @return the (optionally) rewritten property name to match legacy schemas
     */
    public String rewritePropertyName(String basicPropertyName) {
        if (columnAliases != null && columnAliases.containsKey(basicPropertyName)) {
            return columnAliases.get(basicPropertyName);
        }
        return basicPropertyName;
    }

    @Override
    public String toString() {
        return "EntityDescriptor [" + type.getName() + "]";
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

    /**
     * Returns the entity class managed by this descriptor.
     *
     * @return the entity class
     */
    public Class<?> getType() {
        return type;
    }

    /**
     * Returns the property for the given column.
     *
     * @param column the name of the property given as column
     * @return the property which belongs to the given column
     */
    public Property getProperty(Mapping column) {
        if (column.getParent() != null) {
            throw new IllegalArgumentException(Strings.apply("Cannot fetch joined property: %s", column));
        }
        return getProperty(column.getName());
    }

    /**
     * Returns the property for the given name.
     * <p>
     * If the property does not exits, an error will be thrown.
     * </p>
     *
     * @param property the name of the property
     * @return the property with the given name
     */
    @Nonnull
    public Property getProperty(String property) {
        Property prop = properties.get(property.replace('.', '_'));
        if (prop == null) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .withSystemErrorMessage("Cannot find property '%s' for type '%s'.",
                                                    property,
                                                    type.getName())
                            .handle();
        }

        return prop;
    }

    /**
     * Returns the property for the given name.
     * <p>
     * If the property does not exist, <tt>null</tt> is returned.
     * </p>
     *
     * @param property the name of the property or <tt>null</tt> if no property with the given name exists.
     * @return the property with the given name
     */
    @Nullable
    public Property findProperty(String property) {
        return properties.get(property.replace('.', '_'));
    }

    /**
     * Each descriptor keeps a single instance of the entity as reference to determine default values etc.
     *
     * @return the default instance
     */
    public Object getReferenceInstance() {
        return referenceInstance;
    }

    /**
     * Returns the {@link BaseMapper mapper} in charge of managing entities of this type.
     *
     * @return the mapper responsible for entities of this descriptor
     */
    public BaseMapper<?, ?, ?> getMapper() {
        if (mapper == null) {
            try {
                mapper = ((BaseEntity<?>) getReferenceInstance()).getMapper();
            } catch (ClassCastException e) {
                throw Exceptions.handle()
                                .to(Mixing.LOG)
                                .error(e)
                                .withSystemErrorMessage("A mapper was requested for a non-entity descriptor: %s",
                                                        toString())
                                .handle();
            }
        }

        return mapper;
    }

    /**
     * Used to add a class to the list of contained composites within the described entity.
     * <p>
     * This is used by {@link sirius.db.mixing.properties.CompositePropertyFactory} to notify the descriptor that a
     * composite is present.
     *
     * @param composite the type of composite which is present within this entity
     */
    public void addComposite(Class<? extends Composite> composite) {
        composites.add(composite);
    }

    /**
     * Determines if a composite of the given type is present within this entity.
     *
     * @param composite the composite type to check for
     * @return <tt>true</tt> if one or more composites of the given type are present, <tt>false</tt> otherwise
     */
    public boolean hasComposite(Class<? extends Composite> composite) {
        return composites.contains(composite);
    }

    /**
     * Returns all mixins known for the describes entity.
     *
     * @return a collection containing all mixin classes affecting the described entity
     */
    public Collection<Class<? extends Mixable>> getMixins() {
        return mixins;
    }

    /**
     * Provides access to the "legacy" block in the system config which is stored for this entity type.
     *
     * @return the legacy settings in the system config
     */
    public Config getLegacyInfo() {
        return legacyInfo;
    }

    /**
     * Determines if the underlying entity uses optimistic locking.
     *
     * @return <tt>true</tt> if version based optimistic locking is used, <tt>false</tt> otherwise
     */
    public boolean isVersioned() {
        return versioned;
    }
}
