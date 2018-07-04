/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties;

import com.alibaba.fastjson.JSONObject;
import sirius.db.es.ESPropertyInfo;
import sirius.db.es.ElasticEntity;
import sirius.db.es.IndexMappings;
import sirius.db.es.annotations.ESOption;
import sirius.db.es.annotations.IndexMode;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.properties.BaseMapProperty;
import sirius.db.mixing.types.StringLocalDateTimeMap;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Represents an {@link StringLocalDateTimeMap} field within a {@link Mixable}.
 */
public class ESStringLocalDateTimeMapProperty extends BaseMapProperty implements ESPropertyInfo {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return ElasticEntity.class.isAssignableFrom(descriptor.getType())
                   && StringLocalDateTimeMap.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            if (!Modifier.isFinal(field.getModifiers())) {
                Mixing.LOG.WARN("Field %s in %s is not final! This will probably result in errors.",
                                field.getName(),
                                field.getDeclaringClass().getName());
            }

            propertyConsumer.accept(new ESStringLocalDateTimeMapProperty(descriptor, accessPath, field));
        }
    }

    ESStringLocalDateTimeMapProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object transformToDatasource(Object object) {
        Map<Object, Object> result = new HashMap<>();

        if (object instanceof Collection) {
            ((Collection<Map<?, ?>>) object).forEach(entry -> result.put(entry.get(ESStringMapProperty.KEY),
                                                                         entry.get(ESStringMapProperty.VALUE)));
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void setValueToField(Object value, Object target) {
        try {
            StringLocalDateTimeMap map = (StringLocalDateTimeMap) field.get(target);
            map.clear();
            if (value instanceof Map) {
                ((Map<String, Object>) value).forEach((k, v) -> map.put(k, Value.of(v).asLocalDateTime(null)));
            }
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

    @Override
    public void describeProperty(JSONObject description) {
        ESOption indexed = Optional.ofNullable(getClass().getAnnotation(IndexMode.class))
                                   .map(IndexMode::indexed)
                                   .orElse(ESOption.ES_DEFAULT);

        if (ESOption.FALSE == indexed) {
            description.put(IndexMappings.MAPPING_TYPE, "object");
        } else {
            description.put(IndexMappings.MAPPING_TYPE, "nested");
        }
        transferOption(IndexMappings.MAPPING_STORED, IndexMode::stored, ESOption.ES_DEFAULT, description);

        JSONObject properties = new JSONObject();
        properties.put(ESStringMapProperty.KEY,
                       new JSONObject().fluentPut(IndexMappings.MAPPING_TYPE, IndexMappings.MAPPING_TYPE_KEWORD));
        properties.put(ESStringMapProperty.VALUE, new JSONObject().fluentPut(IndexMappings.MAPPING_TYPE, "date"));
        description.put("properties", properties);
        description.put("dynamic", false);
    }
}
