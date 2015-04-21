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
import sirius.mixing.annotations.Length;
import sirius.mixing.annotations.NullAllowed;
import sirius.mixing.schema.Column;
import sirius.mixing.schema.Table;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.util.function.Function;

/**
 * Created by aha on 29.11.14.
 */
public abstract class Property {

    protected String name;
    protected String columnName;
    protected String propertyKey;
    protected String alternativePropertyKey;
    protected String defaultValue;
    protected Field field;
    protected AccessPath accessPath;

    @Part(configPath = "mixing.namingSchema")
    private static NamingSchema namingSchema;

    public Property(@Nonnull AccessPath accessPath, @Nonnull Field field) {
        this.field = field;
        this.propertyKey = field.getDeclaringClass().getSimpleName() + "." + field.getName();
        this.alternativePropertyKey = "Model." + field.getName();
        this.field.setAccessible(true);
        this.name = accessPath.prefix() + field.getName();
        this.columnName = namingSchema.generateColumnName(name);
        this.accessPath = accessPath;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getName() {
        return name;
    }

    public String getLabel() {
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
        specifyColumn(column);
        return column;
    }

    protected boolean isNullable() {
        return !field.getType().isPrimitive() && field.isAnnotationPresent(NullAllowed.class);
    }

    protected int getScale() {
        Length len = field.getAnnotation(Length.class);
        if (len != null) {
            return len.scale();
        }
        return 0;
    }

    protected int getPrecision() {
        Length len = field.getAnnotation(Length.class);
        if (len != null) {
            return len.precision();
        }
        return 0;
    }

    protected int getLength() {
        Length len = field.getAnnotation(Length.class);
        if (len != null) {
            return len.length();
        }
        return 0;
    }

    protected abstract int getSQLType();


    protected void specifyColumn(Column column) {
    }


}
