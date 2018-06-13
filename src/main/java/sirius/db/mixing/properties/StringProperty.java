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
import sirius.db.es.annotations.Analyzed;
import sirius.db.es.annotations.ESOption;
import sirius.db.es.annotations.IndexMode;
import sirius.db.jdbc.schema.SQLPropertyInfo;
import sirius.db.jdbc.schema.Table;
import sirius.db.jdbc.schema.TableColumn;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.annotations.Lob;
import sirius.db.mixing.annotations.Trim;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import java.lang.reflect.Field;
import java.sql.Clob;
import java.sql.Types;
import java.util.function.Consumer;

/**
 * Represents an {@link String} field within a {@link Mixable}.
 */
public class StringProperty extends Property implements SQLPropertyInfo, ESPropertyInfo {

    private final boolean trim;
    private final boolean lob;

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(Field field) {
            return String.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new StringProperty(descriptor, accessPath, field));
        }
    }

    StringProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
        this.trim = field.isAnnotationPresent(Trim.class);
        this.lob = field.isAnnotationPresent(Lob.class);
    }

    @Override
    protected Object transformToDatasource(Object object) {
        if (length > 0 && !lob && object != null && ((String) object).length() > length) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .withNLSKey("StringProperty.dataTruncation")
                            .set("value", Strings.limit(object, 30))
                            .set("field", getLabel())
                            .set("length", ((String) object).length())
                            .set("maxLength", length)
                            .handle();
        }
        return object;
    }

    @Override
    protected void setValueFromDatasource(BaseEntity<?> entity, Value data) {
        Object effectiveValue = data.get();
        if (effectiveValue instanceof Clob) {
            try {
                setValue(entity, ((Clob) effectiveValue).getSubString(1, (int) ((Clob) effectiveValue).length()));
                return;
            } catch (Exception e) {
                throw Exceptions.handle()
                                .to(Mixing.LOG)
                                .error(e)
                                .withSystemErrorMessage("Cannot read CLOB property %s of %s (%s): %s (%s)",
                                                        getName(),
                                                        getDescriptor().getType().getName(),
                                                        getDescriptor().getRelationName())
                                .handle();
            }
        }

        setValue(entity, effectiveValue);
    }

    @Override
    protected void setValueToField(Object value, Object target) {
        if (trim) {
            if (value != null) {
                value = ((String) value).trim();
            }
            if ("".equals(value)) {
                value = null;
            }
        }
        super.setValueToField(value, target);
    }

    @Override
    public Object transformValue(Value value) {
        if (value.isFilled()) {
            return value.asString();
        }
        if (this.isNullable() || Strings.isEmpty(defaultValue)) {
            return null;
        }
        return defaultValue;
    }

    @Override
    public void onBeforeSaveChecks(BaseEntity<?> entity) {
        if (trim) {
            String value = (String) getValue(entity);
            if (value != null) {
                value = value.trim();
                if (value.isEmpty()) {
                    value = null;
                }
                setValue(entity, value);
            }
        }
        super.onBeforeSaveChecks(entity);
    }

    @Override
    public void contributeToTable(Table table) {
        table.getColumns().add(new TableColumn(this, lob ? Types.CLOB : Types.CHAR));
    }

    @Override
    public void describeProperty(JSONObject description) {
        description.put("type", "keyword");
        transferOption(IndexMappings.MAPPING_STORED, IndexMode::stored, ESOption.ES_DEFAULT, description);
        transferOption(IndexMappings.MAPPING_INDEXED, IndexMode::indexed, ESOption.ES_DEFAULT, description);
        transferOption(IndexMappings.MAPPING_DOC_VALUES, IndexMode::indexed, ESOption.ES_DEFAULT, description);
        transferOption(IndexMappings.MAPPING_NORMS, IndexMode::normsEnabled, ESOption.FALSE, description);

        getAnnotation(Analyzed.class).ifPresent(analyzed -> {
            description.put("type", "text");

            if (analyzed.indexOptions() != Analyzed.IndexOption.DEFAULT) {
                description.put("index_options", analyzed.indexOptions().toString().toLowerCase());
            }

            if (Strings.isFilled(analyzed.analyzer())) {
                description.put("analyzer", analyzed.analyzer());
            }
        });
    }
}
