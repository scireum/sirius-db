/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import com.alibaba.fastjson2.JSONObject;
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
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.annotations.Ordinal;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.function.Consumer;

/**
 * Represents an {@link Enum} field within a {@link Mixable}.
 */
public class EnumProperty extends Property implements SQLPropertyInfo, ESPropertyInfo {

    private boolean ordinal;

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return field.getType().isEnum();
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new EnumProperty(descriptor, accessPath, field));
        }
    }

    EnumProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
        this.ordinal = field.isAnnotationPresent(Ordinal.class);
    }

    @SuppressWarnings({"unchecked", "raw", "rawtypes"})
    @Override
    public Object transformValue(Value value) {
        if (value.isFilled()) {
            return value.asEnum((Class<Enum>) field.getType());
        }
        if (this.isNullable() || defaultValue.isEmptyString()) {
            return null;
        }
        return defaultValue.asEnum((Class<Enum>) field.getType());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    protected Object transformValueFromImport(Value value) {
        if (value.isEmptyString()) {
            return null;
        }
        // If a value is present, we check if any constant name or its translation matches...
        String stringValue = value.asString().trim();
        for (Enum enumValue : ((Class<Enum>) field.getType()).getEnumConstants()) {
            // Check for a match of the constant...
            if (Strings.equalIgnoreCase(enumValue.name(), stringValue)) {
                return enumValue;
            }

            // Check of a match of the translation...
            if (Strings.equalIgnoreCase(enumValue.toString(), stringValue)) {
                return enumValue;
            }
        }

        throw illegalFieldValue(value);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void determineLengths() {
        super.determineLengths();

        int maxLength = 0;

        for (Enum<?> e : ((Class<Enum<?>>) field.getType()).getEnumConstants()) {
            maxLength = Math.max(maxLength, e.name().length());
        }

        if (this.length == 0) {
            this.length = maxLength;
        }

        if (maxLength > this.length) {
            Mixing.LOG.SEVERE(Strings.apply(
                    "Length of enum property '%s' (from '%s') isn't large enough to fit maximum size %s",
                    getName(),
                    getDefinition(),
                    maxLength));
        }
    }

    @SuppressWarnings({"unchecked", "raw", "rawtypes"})
    @Override
    public Object transformFromDatasource(Class<? extends BaseMapper<?, ?, ?>> mapperType, Value data) {
        if (data.isNull()) {
            return null;
        }
        if (ordinal) {
            Object[] values = field.getType().getEnumConstants();
            int index = data.asInt(0);
            index = Math.min(index, values.length - 1);
            return values[index];
        }
        return data.asEnum((Class<Enum>) field.getType());
    }

    @Override
    protected Object transformToDatasource(Class<? extends BaseMapper<?, ?, ?>> mapperType, Object object) {
        if (object == null) {
            return null;
        }
        return ordinal ? ((Enum<?>) object).ordinal() : ((Enum<?>) object).name();
    }

    @Override
    public void contributeToTable(Table table) {
        table.getColumns().add(new TableColumn(this, ordinal ? Types.INTEGER : Types.CHAR));
    }

    @Override
    public void describeProperty(JSONObject description) {
        description.put(IndexMappings.MAPPING_TYPE, ordinal ? "integer" : "keyword");
        transferOption(IndexMappings.MAPPING_STORED, getAnnotation(IndexMode.class), IndexMode::stored, description);
        transferOption(IndexMappings.MAPPING_INDEX, getAnnotation(IndexMode.class), IndexMode::indexed, description);
        transferOption(IndexMappings.MAPPING_DOC_VALUES,
                       getAnnotation(IndexMode.class),
                       IndexMode::docValues,
                       description);
        if (!ordinal) {
            transferOption(IndexMappings.MAPPING_NORMS,
                           getAnnotation(IndexMode.class),
                           IndexMode::normsEnabled,
                           description);
        }
    }
}
