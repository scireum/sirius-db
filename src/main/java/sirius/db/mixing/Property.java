/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.es.Elastic;
import sirius.db.jdbc.OMA;
import sirius.db.mixing.annotations.DefaultValue;
import sirius.db.mixing.annotations.Length;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.annotations.Unique;
import sirius.db.mongo.Mango;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;
import sirius.kernel.nls.NLS;

import javax.annotation.Nonnull;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * Maps a field, which is either defined in an entity, a composite or a mixin to a mapped property.
 * <p>
 * A property is responsible for mapping (converting) a value between a field ({@link Field} and a database column.
 * It is also responsible for checking the consistency of this field.
 */
public abstract class Property {

    /**
     * Contains the effective property name. If the field, for which this property was created, resides
     * inside a mixin or composite, the name will be prefixed appropriately. Names are separated by
     * {@link Mapping#SUBFIELD_SEPARATOR} which is a <tt>_</tt>.
     */
    protected String name;

    /**
     * Contains the effective property name. This is normally the same as {@link #name} but can be re-written
     * to support legacy table and column names.
     */
    protected String propertyName;

    /**
     * Represents the name of the property a {@link Mapping}
     */
    protected Mapping nameAsMapping;

    /**
     * Contains a used defined name. This is intended to overwrite property names in customizations.
     *
     * @see #getLabel()
     */
    protected String label;

    /**
     * Contains the i18n key used to determine the label (official name) of the property.
     *
     * @see #getLabel()
     */
    protected String propertyKey;

    /**
     * Contains the alternative i18n key used to determine the label (official name) of the property.
     *
     * @see #getLabel()
     */
    protected String alternativePropertyKey;

    /**
     * Contains the descriptor to which this property belongs
     */
    protected EntityDescriptor descriptor;

    /**
     * Contains the access path used to obtain the target object containing the field
     */
    protected AccessPath accessPath;

    /**
     * The field which generated the property (which stores the column value in the Java world)
     */
    protected Field field;

    /**
     * Contains a string representation of the default value for the column
     */
    protected String defaultValue;

    /**
     * Contains the length of this property
     */
    protected int length = 0;

    /**
     * Determines the nullability of the property
     */
    protected boolean nullable;

    /**
     * Creates a new property for the given descriptor, access path and field.
     *
     * @param descriptor the descriptor which owns the property
     * @param accessPath the access path required to obtain the target object which contains the field
     * @param field      the field which stores the database value
     */
    protected Property(@Nonnull EntityDescriptor descriptor, @Nonnull AccessPath accessPath, @Nonnull Field field) {
        this.descriptor = descriptor;
        this.accessPath = accessPath;
        this.field = field;
        this.propertyKey = field.getDeclaringClass().getSimpleName() + "." + field.getName();
        this.alternativePropertyKey = "Model." + field.getName();
        this.field.setAccessible(true);
        this.name = accessPath.prefix() + field.getName();
        this.propertyName = descriptor.rewritePropertyName(name);
        this.nameAsMapping = Mapping.named(name);

        determineNullability();
        determineLengths();
        determineDefaultValue();
    }

    /**
     * Determines the default value of the column by checking for a {@link DefaultValue} annotation on the field.
     */
    protected void determineDefaultValue() {
        DefaultValue dv = field.getAnnotation(DefaultValue.class);
        if (dv != null) {
            this.defaultValue = dv.value();
        }
    }

    /**
     * Determines the column length by checking for a {@link Length} annotation on the field.
     */
    protected void determineLengths() {
        Length len = field.getAnnotation(Length.class);
        if (len != null) {
            this.length = len.value();
        }
    }

    /**
     * Determines the nullability of the column by checking for a {@link NullAllowed} annotation on the field.
     * <p>
     * Note that subclasses might overwrite this value if they do not accept null values (like properties
     * for primitive types).
     */
    protected void determineNullability() {
        this.nullable = !field.getType().isPrimitive() && field.isAnnotationPresent(NullAllowed.class);
    }

    /**
     * Returns the anotation of the given type.
     *
     * @param type the type of the annotation to fetch
     * @param <A>  the annotation to fetch
     * @return the annotation as optional or <tt>null</tt>, if the field defining the property doesn't wear an
     * annotation of the given type
     */
    public <A extends Annotation> Optional<A> getAnnotation(Class<A> type) {
        return Optional.ofNullable(field.getAnnotation(type));
    }

    /**
     * Returns the name of the property.
     *
     * @return the name of the property.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the effective property name.
     *
     * @return the name of the property in the database
     */
    public String getPropertyName() {
        return propertyName;
    }

    /**
     * Returns the field which will store the database value
     *
     * @return the field in the target object which stores the database value
     */
    @Nonnull
    protected Field getField() {
        return field;
    }

    /**
     * Returns the name of the property which is shown to the user.
     * <p>
     * This can be used in error messages or for labelling in forms.
     * <p>
     * The label can be set in three ways:
     * <ol>
     * <li>Define an i18n value for <tt>propertyKey</tt>, which is normally <tt>[declaingClass].[field]</tt>.
     * So for <tt>com.acme.model.Customer.customerNumber</tt> this would be <tt>Customer.customerNumber</tt></li>
     * <li>Define an i18n value for <tt>alternativePropertyKey</tt>, which is normally <tt>Model.[field]</tt>.
     * So for <tt>com.acme.model.Customer.customerNumber</tt> this would be <tt>Model.customerNumber</tt>. That
     * way common names across different entites can share the same translation.</li>
     * </ol>
     *
     * @return the effective label of the property.
     */
    public String getLabel() {
        if (label != null) {
            return label;
        }
        return NLS.getIfExists(propertyKey, NLS.getCurrentLang()).orElseGet(() -> NLS.get(alternativePropertyKey));
    }

    /**
     * Returns the class name and field name which "defined" this property.
     * <p>
     * This is mainly used to report errors for duplicate names etc.
     *
     * @return the "qualified" field name which "defined" this property
     */
    protected String getDefinition() {
        return field.getDeclaringClass().getName() + "." + field.getName();
    }

    /**
     * Applies the given value to the given entity.
     * <p>
     * The internal access path will be used to find the target object which contains the field.
     * <p>
     * Note that no further value conversion will be performed, therefor the given object must match the expected value.
     * Use {@link #parseValue(Object, Value)} to utilize automatic transformations.
     *
     * @param entity the entity to write to
     * @param object the value to write to the field
     */
    public void setValue(Object entity, Object object) {
        Object target = accessPath.apply(entity);
        setValueToField(object, target);
    }

    /**
     * Applies the given value to the field in the given target object
     *
     * @param value  the database value to store
     * @param target the target object determined by the access path
     */
    protected void setValueToField(Object value, Object target) {
        try {
            field.set(target, value);
        } catch (IllegalAccessException | IllegalArgumentException e) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .error(e)
                            .withSystemErrorMessage("Cannot write property '%s' (from '%s'): %s (%s)",
                                                    getName(),
                                                    getDefinition())
                            .handle();
        }
    }

    /**
     * Applies the given database value to the given entity.
     * <p>
     * The internal access path will be used to find the target object which contains the field.
     * <p>
     * If the underlying field of this property is primitive, but the given value is <tt>null</tt> or transformed to
     * <tt>null</tt>, this will be ignored. An scenario like this might happen, if we join-fetch a value, which is not
     * present.
     *
     * @param mapperType the mapper which is currently active. This can be used to determine which kind of database is
     *                   active and therefore which data format will be available.
     * @param entity     the entity to write to
     * @param data       the database value to store
     */
    protected void setValueFromDatasource(Class<? extends BaseMapper<?, ?, ?>> mapperType, Object entity, Value data) {
        Object effectiveValue = transformFromDatasource(mapperType, data);
        if (field.getType().isPrimitive() && effectiveValue == null) {
            return;
        }

        setValue(entity, effectiveValue);
    }

    /**
     * Converts the database value to the appropriate field value.
     *
     * @param mapperType the mapper which is currently active. This can be used to determine which kind of database is
     *                   active and therefore which data format will be available.
     * @param object     the database value
     * @return the value which can be stored in the associated field
     */
    protected Object transformFromDatasource(Class<? extends BaseMapper<?, ?, ?>> mapperType, Value object) {
        if (mapperType == OMA.class) {
            return transformFromJDBC(object);
        } else if (mapperType == Elastic.class) {
            return transformFromElastic(object);
        } else if (mapperType == Mango.class) {
            return transformFromMongo(object);
        } else {
            throw new UnsupportedOperationException(getClass().getName()
                                                    + " does not yet support: "
                                                    + mapperType.getName());
        }
    }

    /**
     * Loads a value from a JDBC datasource.
     *
     * @param object the database value
     * @return the value which can be stored in the associated field
     */
    protected Object transformFromJDBC(Value object) {
        throw new UnsupportedOperationException(getClass().getName() + " does not yet support JDBC!");
    }

    /**
     * Loads a value from an Elasticsearch database.
     *
     * @param object the database value
     * @return the value which can be stored in the associated field
     */
    protected Object transformFromElastic(Value object) {
        throw new UnsupportedOperationException(getClass().getName() + " does not yet support Elastic!");
    }

    /**
     * Loads a value from a MongoDB datasource.
     *
     * @param object the database value
     * @return the value which can be stored in the associated field
     */
    protected Object transformFromMongo(Value object) {
        throw new UnsupportedOperationException(getClass().getName() + " does not yet support MongoDB!");
    }

    /**
     * Obtains the field value from the given entity.
     * <p>
     * The internal access path will be used to find the target object which contains the field.
     *
     * @param entity the entity to fetch the value from
     * @return the value which is currently stored in the field
     */
    public Object getValue(Object entity) {
        Object target = accessPath.apply(entity);
        return getValueFromField(target);
    }

    /**
     * For modifyable datatypes like collections, this returns the value as copy so that further modifications
     * will not change the returned value.
     *
     * @param entity the entity to fetch the value from
     * @return the as compy of the value which is currently stored in the field
     */
    public Object getValueAsCopy(Object entity) {
        return getValue(entity);
    }

    /**
     * Obtains the value from the field in the given target object
     *
     * @param target the target object determined by the access path
     * @return the object to be stored in the database
     */
    protected Object getValueFromField(Object target) {
        try {
            return field.get(target);
        } catch (IllegalAccessException e) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .error(e)
                            .withSystemErrorMessage("Cannot read property '%s' (from '%s'): %s (%s)",
                                                    getName(),
                                                    getDefinition())
                            .handle();
        }
    }

    /**
     * Obtains the database value from the given entity.
     * <p>
     * The internal access path will be used to find the target object which contains the field.
     *
     * @param mapperType the mapper which is currently active. This can be used to determine which kind of database is
     *                   active and therefore which data format will be required.
     * @param entity     the entity to write to
     * @return the database value to store in the db
     */
    public Object getValueForDatasource(Class<? extends BaseMapper<?, ?, ?>> mapperType, Object entity) {
        return transformToDatasource(mapperType, getValue(entity));
    }

    /**
     * Converts the Java object which resides in the associated field to the database value which is to be
     * written into the database.
     *
     * @param mapperType the mapper which is currently active. This can be used to determine which kind of database is
     *                   active and therefore which data format will be required.
     * @param object     the current field value
     * @return the value which is to be written to the database
     */
    protected Object transformToDatasource(Class<? extends BaseMapper<?, ?, ?>> mapperType, Object object) {
        if (mapperType == OMA.class) {
            return transformToJDBC(object);
        } else if (mapperType == Elastic.class) {
            return transformToElastic(object);
        } else if (mapperType == Mango.class) {
            return transformToMongo(object);
        } else {
            throw new UnsupportedOperationException(getClass().getName()
                                                    + " does not yet support: "
                                                    + mapperType.getName());
        }
    }

    /**
     * Generates a value for a JDBC datasource.
     *
     * @param object the database value
     * @return the value which can be stored in the associated field
     */
    protected Object transformToJDBC(Object object) {
        throw new UnsupportedOperationException(getClass().getName() + " does not yet support JDBC!");
    }

    /**
     * Generates a value for an Elasticsearch database.
     *
     * @param object the database value
     * @return the value which can be stored in the associated field
     */
    protected Object transformToElastic(Object object) {
        throw new UnsupportedOperationException(getClass().getName() + " does not yet support Elastic!");
    }

    /**
     * Generates a value for a MongoDB datasource.
     *
     * @param object the database value
     * @return the value which can be stored in the associated field
     */
    protected Object transformToMongo(Object object) {
        throw new UnsupportedOperationException(getClass().getName() + " does not yet support MongoDB!");
    }

    /**
     * Parses the given value and applies it to the given entity if possible.
     *
     * @param e     the entity to receive the parsed value
     * @param value the value to parse and apply
     */
    public void parseValue(Object e, Value value) {
        try {
            setValue(e, transformValue(value));
        } catch (IllegalArgumentException exception) {
            throw Exceptions.createHandled()
                            .withNLSKey("Property.parseValueErrorMessage")
                            .set("message", exception.getMessage())
                            .error(exception)
                            .handle();
        }
    }

    /**
     * Converts the given value, which most probably contains user input as string into the target type of this
     * property.
     *
     * @param value the value to convert
     * @return the converted value
     */
    public abstract Object transformValue(Value value);

    /**
     * Creates an exception which represents an illegal value for this property
     *
     * @param value the illegal value itself
     * @return an exception filled with an appropriate message
     */
    protected HandledException illegalFieldValue(Value value) {
        return Exceptions.createHandled()
                         .withNLSKey(NLS.fmtr("Property.illegalValue")
                                        .set("property", getLabel())
                                        .set("value", NLS.toUserString(value.get()))
                                        .format())
                         .handle();
    }

    /**
     * Returns the entity descriptor to which this property belongs
     *
     * @return the descriptor which owns this propery
     */
    public EntityDescriptor getDescriptor() {
        return descriptor;
    }

    /**
     * Invoked before an entity is written to the database.
     * <p>
     * Checks the nullability and uniqueness of the property.
     *
     * @param entity the entity to check
     */
    protected final void onBeforeSave(Object entity) {
        onBeforeSaveChecks(entity);
        Object propertyValue = getValue(entity);
        checkNullability(propertyValue);

        if (entity instanceof BaseEntity<?>) {
            if (((BaseEntity<?>) entity).isNew() || ((BaseEntity<?>) entity).isChanged(nameAsMapping)) {
                // Only enforce uniqueness if the value actually changed...
                checkUniqueness(entity, propertyValue);
            }
        }
    }

    /**
     * Invoked before an entity is written to the database.
     * <p>
     * This method is intended to be overwritten with custom logic.
     *
     * @param entity the entity to check
     */
    protected void onBeforeSaveChecks(Object entity) {
    }

    /**
     * Checks if the value is non-null or the property accepts null values.
     *
     * @param propertyValue the value to check
     */
    protected void checkNullability(Object propertyValue) {
        if (!isNullable() && propertyValue == null) {
            throw Exceptions.createHandled().withNLSKey("Property.fieldNotNullable").set("field", getLabel()).handle();
        }
    }

    /**
     * Checks the uniqueness of the given value and entity if an {@link Unique} annotation is present
     *
     * @param entity        the entity to check
     * @param propertyValue the value to check
     */
    protected void checkUniqueness(Object entity, Object propertyValue) {
        Unique unique = field.getAnnotation(Unique.class);
        if (unique == null) {
            return;
        }
        if (!unique.includingNull() && propertyValue == null) {
            return;
        }

        if (!(entity instanceof BaseEntity<?>)) {
            throw new IllegalArgumentException("Only subcalsses of BaseEntity can have unique fields!");
        }

        Mapping[] withinColumns = Arrays.stream(unique.within()).map(Mapping::named).toArray(Mapping[]::new);
        ((BaseEntity<?>) entity).assertUnique(nameAsMapping, propertyValue, withinColumns);
    }

    /**
     * Invoked after an entity was written to the database
     *
     * @param entity the entity which was written to the database
     */
    protected void onAfterSave(Object entity) {
    }

    /**
     * Invoked before an entity is deleted from the database
     *
     * @param entity the entity to be deleted
     */
    protected void onBeforeDelete(Object entity) {
    }

    /**
     * Invoked after an entity was deleted from the database
     *
     * @param entity the entity which was deleted
     */
    protected void onAfterDelete(Object entity) {
    }

    /**
     * Links this property.
     * <p>
     * This is invoked once all <tt>EntityDescriptors</tt> are loaded and can be used to build references to
     * other descriptors / properties.
     */
    protected void link() {
    }

    /**
     * Determines if this property accepts null values
     *
     * @return <tt>true</tt> if this property accepts null values, <tt>false</tt> otherwise
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * Returns the length of the generated column.
     *
     * @return the length of the column or 0 if the associated column has no length
     */
    public int getLength() {
        return length;
    }

    /**
     * Returns the default value to use.
     *
     * @return the default value of this property
     */
    public String getDefaultValue() {
        return defaultValue;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof Property)) {
            return false;
        }

        return descriptor.equals(((Property) obj).descriptor) && Strings.areEqual(name, ((Property) obj).name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(descriptor, name);
    }

    @Override
    public String toString() {
        return name + " [" + getClass().getSimpleName() + "/" + getDefinition() + "]";
    }
}
