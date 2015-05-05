/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing;

import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.nls.NLS;
import sirius.mixing.annotations.DefaultValue;
import sirius.mixing.annotations.Length;
import sirius.mixing.annotations.NullAllowed;
import sirius.mixing.annotations.Unique;
import sirius.mixing.schema.Table;
import sirius.mixing.schema.TableColumn;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;

/**
 * Created by aha on 29.11.14.
 */
public abstract class Property {

    protected String name;
    protected Column nameAsColumn;

    protected String label;
    protected String propertyKey;
    protected String alternativePropertyKey;


    protected EntityDescriptor descriptor;
    protected AccessPath accessPath;
    protected Field field;

    protected String defaultValue;
    protected int length = 0;
    protected int scale = 0;
    protected int precision = 0;
    protected boolean nullable;

    public Property(@Nonnull EntityDescriptor descriptor, @Nonnull AccessPath accessPath, @Nonnull Field field) {
        this.descriptor = descriptor;
        this.accessPath = accessPath;
        this.field = field;
        this.propertyKey = field.getDeclaringClass().getSimpleName() + "." + field.getName();
        this.alternativePropertyKey = "Model." + field.getName();
        this.field.setAccessible(true);
        this.name = accessPath.prefix() + field.getName();
        this.nameAsColumn = Column.named(name);
        determineNullability();
        determineLengths();
        determineDefaultValue();
    }

    protected void determineDefaultValue() {
        DefaultValue dv = field.getAnnotation(DefaultValue.class);
        if (dv != null) {
            this.defaultValue = dv.value();
        }
    }

    protected void determineLengths() {
        Length len = field.getAnnotation(Length.class);
        if (len != null) {
            this.length = len.length();
            this.scale = len.scale();
            this.precision = len.precision();
        }
    }

    protected void determineNullability() {
        this.nullable = field.getType().isPrimitive() && field.isAnnotationPresent(NullAllowed.class);
    }

    public String getColumnName() {
        return name;
    }

    public String getName() {
        return name;
    }

    public Field getField() {
        return field;
    }

    public String getLabel() {
        if (label != null) {
            return label;
        }
        return NLS.getIfExists(propertyKey, NLS.getCurrentLang()).orElseGet(() -> NLS.get(alternativePropertyKey));
    }

    public String getDefinition() {
        return field.getDeclaringClass().getName() + "." + field.getName();
    }

    public void setValue(Entity entity, Object object) {
        try {
            Object target = accessPath.apply(entity);
            setValueToField(object, target);
        } catch (Throwable e) {
            //TODO
            throw Exceptions.handle(e);
        }
    }

    protected void setValueToField(Object value, Object target) throws Exception {
        field.set(target, transformFromColumn(value));
    }

    public Object getValue(Entity entity) {
        try {
            Object target = accessPath.apply(entity);
            Object valueFromField = getValueFromField(target);
            return transformToColumn(valueFromField);
        } catch (Throwable e) {
            //TODO
            throw Exceptions.handle(e);
        }
    }

    protected Object getValueFromField(Object target) throws Exception {
        return field.get(target);
    }

    protected Object transformFromColumn(Object object) {
        return object;
    }

    protected Object transformToColumn(Object object) {
        return object;
    }

    protected void onBeforeSave(Entity entity) {
        Object propertyValue = getValue(entity);
        checkNullability(propertyValue);
        checkUniqueness(entity, propertyValue);
    }

    protected void checkNullability(Object propertyValue) {
        if (!isNullable() && propertyValue == null) {
            throw Exceptions.createHandled().withNLSKey("Property.fieldNotNullable").set("field", getLabel()).handle();
        }
    }

    @Part
    protected static OMA oma;

    protected void checkUniqueness(Entity entity, Object propertyValue) {
        Unique unique = field.getAnnotation(Unique.class);
        if (unique == null) {
            return;
        }
        if (!unique.includingNull() && propertyValue == null) {
            return;
        }
        SmartQuery<? extends Entity> qry = oma.select(descriptor.getType()).eq(nameAsColumn, propertyValue);
        for (String field : unique.within()) {
            qry.eq(Column.named(field), descriptor.getProperty(field).getValue(entity));
        }
        Entity other = qry.queryFirst();
        if (other != null && !other.equals(entity)) {
            throw Exceptions.createHandled()
                            .withNLSKey("Property.fieldNotUnique")
                            .set("field", getLabel())
                            .set("value", NLS.toUserString(propertyValue))
                            .handle();
        }
    }

    protected void onAfterSave(Entity entity) {

    }

    protected void onBeforeDelete(Entity entity) {

    }

    protected void onAfterDelete(Entity entity) {

    }

    protected void contributeToTable(Table table) {
        table.getColumns().add(createColumn());
    }

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
        finalizeColumn(column);
        return column;
    }

    protected boolean isNullable() {
        return nullable;
    }

    protected int getScale() {
        return scale;
    }

    protected int getPrecision() {
        return precision;
    }

    protected int getLength() {
        return length;
    }

    protected abstract int getSQLType();


    protected void finalizeColumn(TableColumn column) {
    }

    protected void link() {
    }

    public EntityDescriptor getDescriptor() {
        return descriptor;
    }

}
