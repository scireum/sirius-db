/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.mixing.annotations.DefaultValue;
import sirius.db.mixing.annotations.Length;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.annotations.Numeric;
import sirius.db.mixing.annotations.Unique;
import sirius.db.mixing.schema.Table;
import sirius.db.mixing.schema.TableColumn;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;
import sirius.kernel.nls.NLS;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Maps a field, which is either defined in an entity, a composite or a mixin to a table column.
 * <p>
 * A property is responsible for mapping (converting) a value between a field ({@link Field} and a database column.
 * It is also responsible for checking the consistency of this field.
 */
public abstract class Property {

    /**
     * Contains the effective property name. If the field, for which this property was created, resides
     * inside a mixin or composite, the name will be prefixed appropriately. Names are separated by
     * {@link Column#SUBFIELD_SEPARATOR} which is a <tt>_</tt>.
     */
    protected String name;

    /**
     * Contains the effective column name. This is normally the same as {@link #name} but can be re-written
     * to support legacy table and column names.
     */
    protected String columnName;

    /**
     * Represents the name of the property a {@link Column}
     */
    protected Column nameAsColumn;

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
     * Contains the length of the database column
     */
    protected int length = 0;

    /**
     * Contains the scale of the database column
     */
    protected int scale = 0;

    /**
     * Contains the precision of the database column
     */
    protected int precision = 0;

    /**
     * Determines the nullability of the database column
     */
    protected boolean nullable;

    /**
     * Creates a new property for the given descriptor, access path and field.
     * <p>
     * Fills the column description by checking for a {@link Length} annotation. Also initializes <tt>propertyKey</tt>
     * and <tt>alternativePropertyKey</tt> and computes the property name based on the field name and access path.
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
        this.columnName = descriptor.rewriteColumnName(name);
        this.nameAsColumn = Column.named(columnName);

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
        Numeric num = field.getAnnotation(Numeric.class);
        if (num != null) {
            this.scale = num.scale();
            this.precision = num.precision();
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
    @Nullable
    public <A extends Annotation> A getAnnotation(Class<A> type) {
        return field.getAnnotation(type);
    }

    /**
     * Returns the effective column name.
     *
     * @return the name of the column in the database
     */
    public String getColumnName() {
        return columnName;
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
     * <li>Set the <tt>label</tt> field using {@link #setLabel(String)}
     * (e.g. via a {@link PropertyModifier}).</li>
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
     * Converts the database value which comes from the JDBC driver to the appropriate field value.
     *
     * @param object the database value
     * @return the value which can be stored in the associated field
     */
    protected Object transformFromColumn(Object object) {
        return object;
    }

    /**
     * Applies the database value to the field in the given target object
     *
     * @param value  the database value to store
     * @param target the target object determined by the access path
     */
    protected void setValueToField(Object value, Object target) {
        try {
            field.set(target, value);
        } catch (IllegalAccessException e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("Cannot write property '%s' (from '%s'): %s (%s)",
                                                    getName(),
                                                    getDefinition())
                            .handle();
        }
    }

    /**
     * Applies the given value to the given entity.
     * <p>
     * The internal access path will be used to find the target object which contains the field.
     *
     * @param entity the entity to write to
     * @param object the value to write to the field
     */
    protected void setValue(Entity entity, Object object) {
        Object target = accessPath.apply(entity);
        setValueToField(object, target);
    }

    /**
     * Applies the given datbase value to the given entity.
     * <p>
     * The internal access path will be used to find the target object which contains the field.
     *
     * @param entity the entity to write to
     * @param object the database value to store
     */
    protected void setValueFromColumn(Entity entity, Object object) {
        setValue(entity, transformFromColumn(object));
    }

    /**
     * Converts the Java object which resides in the associated field to the database value which is to be
     * written into the database via JDBC.
     *
     * @param object the current field value
     * @return the value which is to be written to the database
     */
    protected Object transformToColumn(Object object) {
        return object;
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
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage("Cannot read property '%s' (from '%s'): %s (%s)",
                                                    getName(),
                                                    getDefinition())
                            .handle();
        }
    }

    /**
     * Obtains the field value from the given entity.
     * <p>
     * The internal access path will be used to find the target object which contains the field.
     *
     * @param entity the entity to write to
     * @return the value which is currently stored in the field
     */
    public Object getValue(Entity entity) {
        Object target = accessPath.apply(entity);
        return getValueFromField(target);
    }

    /**
     * Obtains the database value from the given entity.
     * <p>
     * The internal access path will be used to find the target object which contains the field.
     *
     * @param entity the entity to write to
     * @return the database value to store in the db
     */
    protected Object getValueForColumn(Entity entity) {
        return transformToColumn(getValue(entity));
    }

    /**
     * Parses the given value and applies it to the given entity if possible.
     *
     * @param e     the entity to receive the parsed value
     * @param value the value to parse and apply
     */
    public void parseValue(Entity e, Value value) {
        setValue(e, transformValue(value));
    }

    /**
     * Converts the given value into the target type of this property
     *
     * @param value the value to convert
     * @return the converted value
     */
    protected abstract Object transformValue(Value value);

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
    protected final void onBeforeSave(Entity entity) {
        onBeforeSaveChecks(entity);
        Object propertyValue = getValue(entity);
        checkNullability(propertyValue);
        checkUniqueness(entity, propertyValue);
    }

    /**
     * Invoked before an entity is written to the database.
     * <p>
     * This method is intended to be overwritten with custom logic.
     *
     * @param entity the entity to check
     */
    protected void onBeforeSaveChecks(Entity entity) {
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

    @Part
    protected static OMA oma;

    /**
     * Checks the uniqueness of the given value and entity if an {@link Unique} annotation is present
     *
     * @param entity        the entity to check
     * @param propertyValue the value to check
     */
    protected void checkUniqueness(Entity entity, Object propertyValue) {
        Unique unique = field.getAnnotation(Unique.class);
        if (unique == null) {
            return;
        }
        if (!unique.includingNull() && propertyValue == null) {
            return;
        }

        List<Column> withinColumns = Arrays.stream(unique.within()).map(Column::named).collect(Collectors.toList());
        entity.assertUnique(nameAsColumn, propertyValue, withinColumns.toArray(new Column[withinColumns.size()]));
    }

    /**
     * Invoked after an entity was written to the database
     *
     * @param entity the entity which was written to the database
     */
    protected void onAfterSave(Entity entity) {
    }

    /**
     * Invoked before an entity is deleted from the database
     *
     * @param entity the entity to be deleted
     */
    protected void onBeforeDelete(Entity entity) {
    }

    /**
     * Invoked after an entity was deleted from the database
     *
     * @param entity the entity which was deleted
     */
    protected void onAfterDelete(Entity entity) {
    }

    /**
     * Appends columns, keys and foreign keys to the given table to match the settings specified by
     * this property
     *
     * @param table the table to add schema infos to
     */
    protected void contributeToTable(Table table) {
        table.getColumns().add(createColumn());
    }

    /**
     * Create the <tt>TableColumn</tt> which is added to the table by {@link #contributeToTable(Table)}
     *
     * @return a table column which describes the database column which represents this property
     */
    protected TableColumn createColumn() {
        TableColumn column = new TableColumn();
        column.setName(getColumnName());
        if (defaultValue != null) {
            column.setDefaultValue(defaultValue);
        }
        column.setType(getSQLType());
        column.setNullable(isNullable());
        if (getLength() > 0) {
            column.setLength(getLength());
        }
        if (getPrecision() > 0) {
            column.setPrecision(getPrecision());
        }
        if (getScale() > 0) {
            column.setScale(getScale());
        }
        return column;
    }

    /**
     * Returns the JDBC column type used as database column
     *
     * @return the JDBC column type used to store this property
     * @see java.sql.Types
     */
    protected abstract int getSQLType();

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
    protected boolean isNullable() {
        return nullable;
    }

    /**
     * Returns the number of digits following the decimal point if the underlying column is a DECIMAL.
     *
     * @return the scale of the column
     */
    protected int getScale() {
        return scale;
    }

    /**
     * Returns the number of significat digits stored for this column if it is DECIMAL.
     *
     * @return the total number (including those following the decimal point) of
     * significant digits stored for the column
     */
    protected int getPrecision() {
        return precision;
    }

    /**
     * Returns the length of the generated column.
     *
     * @return the length of the column or 0 if the associated column has no length
     */
    protected int getLength() {
        return length;
    }

    /**
     * Explicitely sets the label for this column.
     * <p>
     * This should only be used to customizations, as the label will not be translated anymore.
     * See <tt>getLabel()</tt> on how to set a label for a column.
     *
     * @param label the new label of the column
     * @see #getLabel()
     */
    public void setLabel(String label) {
        this.label = label;
    }

    /**
     * Can be used by a {@link PropertyModifier} to overwrite the length of the column.
     * <p>
     * Normally, the column length is specified by the type or via a {@link Length} annotation at the field.
     *
     * @param length the new length of the column
     */
    public void setLength(int length) {
        this.length = length;
    }

    /**
     * Can be used by a {@link PropertyModifier} to overwrite the default value of the column.
     * <p>
     * Normally, the default value is specified via a {@link DefaultValue} annotation at the field.
     *
     * @param defaultValue the new default value of the column
     */
    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    /**
     * Can be used by a {@link PropertyModifier} to overwrite the scale value of the column.
     * <p>
     * Normally, the column scale is specified by the type or via a {@link Length} annotation at the field.
     *
     * @param scale the new scale of the column
     */
    public void setScale(int scale) {
        this.scale = scale;
    }

    /**
     * Can be used by a {@link PropertyModifier} to overwrite the precision value of the column.
     * <p>
     * Normally, the column precision is specified by the type or via a {@link Length} annotation at the field.
     *
     * @param precision the new precision of the column
     */
    public void setPrecision(int precision) {
        this.precision = precision;
    }

    /**
     * Can be used by a {@link PropertyModifier} to overwrite the nullability value of the column,
     * if permitted by the type.
     * <p>
     * Normally, the column nullability is specified via a {@link NullAllowed} annotation at the field. Note that if
     * the type of the property does not handle null values (i.e. primitive fields), calling this method has no effect.
     *
     * @param nullable the new nullability setting of the column
     */
    public void setNullable(boolean nullable) {
        this.nullable = nullable;
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
