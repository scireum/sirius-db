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
import sirius.db.es.annotations.ESOption;
import sirius.db.es.annotations.IndexMode;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.annotations.Ordinal;
import sirius.db.jdbc.schema.SQLPropertyInfo;
import sirius.db.jdbc.schema.Table;
import sirius.db.jdbc.schema.TableColumn;
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
        if (this.isNullable() || Strings.isEmpty(defaultValue)) {
            return null;
        }
        return Value.of(defaultValue).asEnum((Class<Enum>) field.getType());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void determineLengths() {
        for (Enum<?> e : ((Class<Enum<?>>) field.getType()).getEnumConstants()) {
            this.length = Math.max(this.length, e.name().length());
        }
    }

    @SuppressWarnings({"unchecked", "raw", "rawtypes"})
    @Override
    protected Object transformFromDatasource(Value data) {
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
    protected Object transformToDatasource(Object object) {
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
        transferOption(IndexMappings.MAPPING_STORED, IndexMode::stored, ESOption.ES_DEFAULT, description);
        transferOption(IndexMappings.MAPPING_INDEXED, IndexMode::indexed, ESOption.ES_DEFAULT, description);
        transferOption(IndexMappings.MAPPING_DOC_VALUES, IndexMode::indexed, ESOption.ES_DEFAULT, description);
        if (!ordinal) {
            transferOption(IndexMappings.MAPPING_NORMS, IndexMode::normsEnabled, ESOption.FALSE, description);
        }
    }
}
