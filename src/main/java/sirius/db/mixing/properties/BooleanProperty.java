/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import com.alibaba.fastjson.JSONObject;
import sirius.db.es.ESPropertyInfo;
import sirius.db.es.IndexMappings;
import sirius.db.es.annotations.IndexMode;
import sirius.db.jdbc.schema.SQLPropertyInfo;
import sirius.db.jdbc.schema.Table;
import sirius.db.jdbc.schema.TableColumn;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.nls.NLS;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.function.Consumer;

/**
 * Represents a {@link Boolean} field within a {@link Mixable}.
 */
public class BooleanProperty extends Property implements ESPropertyInfo, SQLPropertyInfo {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return Boolean.class.equals(field.getType()) || boolean.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new BooleanProperty(descriptor, accessPath, field));
        }
    }

    BooleanProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    public Object transformValue(Value value) {
        if (defaultValue == null) {
            if (value.isNull() || value.isEmptyString()) {
                return null;
            } else {
                return value.asBoolean();
            }
        }

        return value.asBoolean(NLS.parseMachineString(Boolean.class, defaultValue));
    }

    @Override
    public Object transformFromDatasource(Class<? extends BaseMapper<?, ?, ?>> mapperType, Value data) {
        Object object = data.get();
        if (object instanceof Boolean) {
            return object;
        }

        if (data.is(String.class)) {
            return data.asBoolean();
        }

        if (object == null) {
            if (field.getType().isPrimitive()) {
                return false;
            } else {
                return null;
            }
        }

        return ((Integer) object) != 0;
    }

    @Override
    protected void determineDefaultValue() {
        super.determineDefaultValue();
        if (defaultValue == null && field.getType().isPrimitive()) {
            this.defaultValue = "0";
        }
    }

    @Override
    protected Object transformToJDBC(Object object) {
        if (object == null) {
            return null;
        }

        return Boolean.TRUE.equals(object) ? 1 : 0;
    }

    @Override
    protected Object transformToElastic(Object object) {
        return object;
    }

    @Override
    protected Object transformToMongo(Object object) {
        return object;
    }

    @Override
    public void contributeToTable(Table table) {
        TableColumn tableColumn = new TableColumn(this, Types.BOOLEAN);
        tableColumn.setLength(1);
        table.getColumns().add(tableColumn);
    }

    @Override
    public void describeProperty(JSONObject description) {
        description.put(IndexMappings.MAPPING_TYPE, "boolean");
        transferOption(IndexMappings.MAPPING_STORED, getAnnotation(IndexMode.class), IndexMode::stored, description);
        transferOption(IndexMappings.MAPPING_INDEX, getAnnotation(IndexMode.class), IndexMode::indexed, description);
        transferOption(IndexMappings.MAPPING_DOC_VALUES,
                       getAnnotation(IndexMode.class),
                       IndexMode::docValues,
                       description);
    }
}
