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
import sirius.kernel.commons.Values;
import sirius.kernel.di.transformers.Composable;
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
public abstract class Property extends Composable {

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
     * Contains the i18n key used to determine the label (official name) of the property.
     * <p>
     * This is built like <tt>SimpleClassNameDefiningTheField.fieldName</tt>
     *
     * @see #getLabel()
     */
    protected String propertyKey;

    /**
     * Contains a class local i18n key used to determine the label (official name) of the property.
     * <p>
     * This is built like <tt>EntityTypeName.compositeOrMixinField_fieldName</tt>
     * <p>
     * This can be used to provide different labels for the same field of a composite based on
     * the class where the composite is included.
     *
     * @see #getLabel()
     */
    protected String localPropertyKey;

    /**
     * Contains a class local i18n key used to determine the fullLabel of the property.
     * <p>
     * The fullLabel is the label which is used in error messages.
     * <p>
     * If this property resides in a composite or mixin, this parent property key is filled
     * and will be used build a full name.
     */
    protected String parentPropertyKey;

    /**
     * Contains the alternative i18n key used to determine the label (official name) of the property.
     * <p>
     * This is built like <tt>Model.fieldName</tt>.
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
        this.propertyKey = EntityDescriptor.determineTranslationSource(field.getDeclaringClass()).getSimpleName()
                           + "."
                           + field.getName();
        this.alternativePropertyKey = "Model." + field.getName();
        this.field.setAccessible(true);
        this.name = accessPath.qualify(field.getName());
        if (Strings.isFilled(accessPath.prefix())) {
            this.localPropertyKey = descriptor.getTranslationSource().getSimpleName() + "." + name;
            this.parentPropertyKey = descriptor.getTranslationSource().getSimpleName() + "." + accessPath.prefix();
        }
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
     * Determines if an anotation of the given type is present.
     *
     * @param type the type of the annotation to fetch
     * @return <tt>true</tt> if an annotation of the given type is present, <tt>false</tt> otherwise
     */
    public boolean isAnnotationPresent(Class<? extends Annotation> type) {
        return field.isAnnotationPresent(type);
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
     * Returns the field which will store the database value.
     *
     * @return the field in the target object which stores the database value
     */
    @Nonnull
    public Field getField() {
        return field;
    }

    /**
     * Executes the underlying access path to obtain the object out of the entity which actually contains the field.
     * <p>
     * This object might differ from the entity itself for composites and mixins.
     *
     * @param entity the entity to read the target object from
     * @return the target object which actually contains the property
     */
    public Object getTarget(Object entity) {
        return accessPath.apply(entity);
    }

    /**
     * Returns the name of the property which is shown to the user.
     * <p>
     * The label can be set in three ways:
     * <ol>
     * <li>
     * Define an local i18n value for <tt>propertyKey</tt>, which is normally <tt>[entityClass].[compositeName]_[field]</tt>.
     * So for <tt>com.acme.model.Address.street</tt> in <tt>com.acme.model.Customer</tt> this would be
     * <tt>Customer.address_street</tt>. This can be used to give the same composite field different
     * names of a per-entity basis.
     * </li>
     * <li>
     * Define an i18n value for <tt>propertyKey</tt>, which is normally <tt>[declaingClass].[field]</tt>.
     * So for <tt>com.acme.model.Address.street</tt> this would be <tt>Address.street</tt>
     * </li>
     * <li>
     * Define an i18n value for <tt>alternativePropertyKey</tt>, which is normally <tt>Model.[field]</tt>.
     * So for <tt>com.acme.model.Address.street</tt> this would be <tt>Model.street</tt>. That
     * way common names across different entities can share the same translation.
     * </li>
     * </ol>
     *
     * @return the effective label of the property
     * @see #getFullLabel()
     */
    public String getLabel() {
        String currentLang = NLS.getCurrentLang();
        String localLabel = NLS.getIfExists(localPropertyKey, currentLang).orElse(null);
        if (Strings.isFilled(localLabel)) {
            return localLabel;
        }

        return NLS.getIfExists(propertyKey, currentLang)
                  .orElseGet(() -> NLS.getIfExists(alternativePropertyKey, currentLang)
                                      .orElseGet(() -> NLS.get(propertyKey)));
    }

    /**
     * Returns the full label or nme of the property which is shown in error messages etc.
     * <p>
     * This will only differ from {@link #getLabel()} for field in composites or mixins. In this case,
     * we try to lookup the "parent" name (<tt>[entityClass].[compositeName]</tt>), that is the access path leading
     * to this field. If a property is available for this and none is present for the fully qualified name
     * (<tt>[entityClass].[compositeName]_[field]</tt>), a label in the form of <tt>getLabel() (NLS.get(parent))</tt>
     * is shown.
     *
     * @return the effective full label for the property
     * @see #getLabel()
     */
    public String getFullLabel() {
        String currentLang = NLS.getCurrentLang();
        String result = NLS.getIfExists(localPropertyKey, currentLang).orElse(null);
        if (Strings.isFilled(result)) {
            return result;
        }

        if (Strings.isFilled(parentPropertyKey)) {
            String parentLabel = NLS.getIfExists(parentPropertyKey, currentLang).orElse(null);
            if (Strings.isFilled(parentLabel)) {
                return Strings.apply("%s (%s)", getLabel(), parentLabel);
            }
        }

        return getLabel();
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
     * Note that no further value conversion will be performed, therefore the given object must match the expected value.
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
    public Object transformFromDatasource(Class<? extends BaseMapper<?, ?, ?>> mapperType, Value object) {
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
                            .error(new InvalidFieldException(getName()))
                            .withNLSKey("Property.parseValueErrorMessage")
                            .set("message", exception.getMessage())
                            .error(exception)
                            .handle();
        }
    }

    /**
     * Parses the given values and applies it to the given entity if possible.
     *
     * @param e      the entity to receive the parsed value
     * @param values the values to parse and apply
     */
    public void parseValues(Object e, Values values) {
        parseValue(e, values.at(0));
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
     * Parses the given value read from an import and applies it to the given entity if possible.
     * <p>
     * In contrast to {@link #parseValue(Object, Value)} which is intended for user input, this method should
     * be used to process data from an import (e.g. an Excel import).
     *
     * @param e     the entity to receive the parsed value
     * @param value the value to parse and apply
     */
    public void parseValueFromImport(Object e, Value value) {
        try {
            setValue(e, transformValueFromImport(value));
        } catch (IllegalArgumentException exception) {
            throw Exceptions.createHandled()
                            .error(new InvalidFieldException(getName()))
                            .withNLSKey("Property.parseValueErrorMessage")
                            .set("message", exception.getMessage())
                            .error(exception)
                            .handle();
        }
    }

    /**
     * Converts the given value, which is read from an import (e.g. an Excel import) into the target type of this
     * property.
     * <p>
     * By default, this will use the logic provided by {@link #transformValue(Value)} but cen be overwritten by
     * properties.
     *
     * @param value the value to convert
     * @return the converted value
     */
    protected Object transformValueFromImport(Value value) {
        return transformValue(value);
    }

    /**
     * Creates an exception which represents an illegal value for this property
     *
     * @param value the illegal value itself
     * @return an exception filled with an appropriate message
     */
    protected HandledException illegalFieldValue(Value value) {
        return Exceptions.createHandled()
                         .error(new InvalidFieldException(getName()))
                         .withNLSKey("Property.illegalValue")
                         .set("property", getFullLabel())
                         .set("value", NLS.toUserString(value.get()))
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

        if (entity instanceof BaseEntity<?> && (((BaseEntity<?>) entity).isNew() || ((BaseEntity<?>) entity).isChanged(
                nameAsMapping))) {
            // Only enforce uniqueness if the value actually changed...
            checkUniqueness(entity, propertyValue);
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
        if (!isNullable() && isConsideredNull(propertyValue)) {
            throw Exceptions.createHandled()
                            .error(new InvalidFieldException(getName()))
                            .withNLSKey("Property.fieldNotNullable")
                            .set("field", getFullLabel())
                            .handle();
        }
    }

    /**
     * Determines if the given value is considered to be <tt>null</tt>.
     *
     * @param propertyValue the value to check
     */
    protected boolean isConsideredNull(Object propertyValue) {
        return propertyValue == null;
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
            throw new IllegalArgumentException("Only subclasses of BaseEntity can have unique fields!");
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
