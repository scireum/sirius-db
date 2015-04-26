/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.properties;

import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.nls.NLS;
import sirius.mixing.Entity;
import sirius.mixing.NamingSchema;
import sirius.mixing.annotations.DefaultValue;
import sirius.mixing.annotations.Length;
import sirius.mixing.annotations.NullAllowed;
import sirius.mixing.schema.Column;
import sirius.mixing.schema.Table;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;

/**
 * Created by aha on 29.11.14.
 */
public abstract class Property {

    protected String name;
    protected String columnName;

    protected String label;
    protected String propertyKey;
    protected String alternativePropertyKey;

    protected AccessPath accessPath;
    protected Field field;

    protected String defaultValue;
    protected int length = 0;
    protected int scale = 0;
    protected int precision = 0;
    protected boolean nullable;

    @Part(configPath = "mixing.namingSchema")
    private static NamingSchema namingSchema;

    public Property(@Nonnull AccessPath accessPath, @Nonnull Field field) {
        this.accessPath = accessPath;
        this.field = field;
        this.propertyKey = field.getDeclaringClass().getSimpleName() + "." + field.getName();
        this.alternativePropertyKey = "Model." + field.getName();
        this.field.setAccessible(true);
        this.name = accessPath.prefix() + field.getName();
        this.columnName = namingSchema.generateColumnName(name);
        determineNullability();
        determineLengths();
        determineDefaultValue();
    }

    private void determineDefaultValue() {
        DefaultValue dv = field.getAnnotation(DefaultValue.class);
        if (dv != null) {
            this.defaultValue = dv.value();
        }
    }

    private void determineLengths() {
        Length len = field.getAnnotation(Length.class);
        if (len != null) {
            this.length = len.length();
            this.scale = len.scale();
            this.precision = len.precision();
        }
    }

    private void determineNullability() {
        this.nullable = field.getType().isPrimitive() && field.isAnnotationPresent(NullAllowed.class);
    }

    public String getColumnName() {
        return columnName;
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
            field.set(target, transformFromColumn(object));
        } catch (Throwable e) {
            //TODO
            throw Exceptions.handle(e);
        }
    }

    public Object getValue(Entity entity) {
        try {
            Object target = accessPath.apply(entity);
            return transformToColumn(field.get(target));
        } catch (Throwable e) {
            //TODO
            throw Exceptions.handle(e);
        }
    }

    protected Object transformFromColumn(Object object) {
        return object;
    }

    protected Object transformToColumn(Object object) {
        return object;
    }

    public void onBeforeSave(Entity entity) {

    }

    public void onAfterSave(Entity entity) {

    }

    public void onBeforeDelete(Entity entity) {

    }

    public void onAfterDelete(Entity entity) {

    }

    public void addColumns(Table table) {
        table.getColumns().add(createColumn());
    }

    protected Column createColumn() {
        Column column = new Column();
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


    protected void finalizeColumn(Column column) {
    }


}
