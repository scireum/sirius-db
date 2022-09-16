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
import sirius.db.mixing.annotations.Lob;
import sirius.db.mixing.annotations.LowerCase;
import sirius.db.mixing.annotations.Trim;
import sirius.db.mixing.annotations.UpperCase;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.sql.Clob;
import java.sql.Types;
import java.util.function.Consumer;

/**
 * Represents an {@link String} field within a {@link Mixable}.
 */
public class StringProperty extends Property implements SQLPropertyInfo, ESPropertyInfo {

    private final boolean trim;
    private final boolean lowerCase;
    private final boolean upperCase;
    private final boolean lob;

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
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

    protected StringProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
        this.trim = field.isAnnotationPresent(Trim.class);
        this.lowerCase = field.isAnnotationPresent(LowerCase.class);
        this.upperCase = field.isAnnotationPresent(UpperCase.class);
        this.lob = field.isAnnotationPresent(Lob.class);
    }

    @Override
    protected Object transformToDatasource(Class<? extends BaseMapper<?, ?, ?>> mapperType, Object object) {
        return object;
    }

    @Override
    protected Object transformFromJDBC(Value object) {
        Object effectiveValue = object.get();
        if (effectiveValue instanceof Clob clob) {
            try {
                return clob.getSubString(1, (int) clob.length());
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
        return effectiveValue;
    }

    @Override
    protected Object transformFromElastic(Value object) {
        return object.get();
    }

    @Override
    protected Object transformFromMongo(Value object) {
        return object.get();
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
        if (this.isNullable() || defaultValue.isEmptyString()) {
            return null;
        }
        return defaultValue.asString();
    }

    @Override
    public void onBeforeSaveChecks(Object entity) {
        String value = (String) getValue(entity);
        if (value != null && (trim || lowerCase || upperCase)) {
            value = applyAnnotationModifications(entity, value);
            setValue(entity, value);
        }

        super.onBeforeSaveChecks(entity);

        if (length > 0 && value != null && value.length() > length) {
            throw Exceptions.createHandled()
                            .withNLSKey("StringProperty.dataTruncation")
                            .set("value", Strings.limit(value, 30))
                            .set("field", getFullLabel())
                            .set("length", value.length())
                            .set("maxLength", length)
                            .handle();
        }
    }

    @Nullable
    private String applyAnnotationModifications(Object entity, String value) {
        String modifiedValue = value;

        if (trim) {
            modifiedValue = modifiedValue.trim();
        }
        if (modifiedValue.isEmpty()) {
            modifiedValue = null;
        } else if (lowerCase) {
            modifiedValue = modifiedValue.toLowerCase();
        } else if (upperCase) {
            modifiedValue = modifiedValue.toUpperCase();
        }

        return modifiedValue;
    }

    @Override
    public void contributeToTable(Table table) {
        table.getColumns().add(new TableColumn(this, lob ? Types.CLOB : Types.CHAR));
    }

    @Override
    public void describeProperty(JSONObject description) {
        description.put("type", "keyword");
        transferOption(IndexMappings.MAPPING_STORED, getAnnotation(IndexMode.class), IndexMode::stored, description);
        transferOption(IndexMappings.MAPPING_INDEX, getAnnotation(IndexMode.class), IndexMode::indexed, description);
        transferOption(IndexMappings.MAPPING_DOC_VALUES,
                       getAnnotation(IndexMode.class),
                       IndexMode::docValues,
                       description);
        transferOption(IndexMappings.MAPPING_NORMS,
                       getAnnotation(IndexMode.class),
                       IndexMode::normsEnabled,
                       description);

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
