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
import sirius.db.mixing.StringMap;
import sirius.db.mixing.properties.BaseMapProperty;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Represents an {@link StringMap } field within a {@link Mixable}.
 * <p>
 * Note that maps are stored as a list of nested objects which contain a <tt>key</tt> and
 * a <tt>value</tt>. This is used to prevent a mapping explosion within ES. whil still permitting to
 * search and filter using nested queries.
 */
public class ESStringMapProperty extends BaseMapProperty implements ESPropertyInfo {

    protected static final String KEY = "key";
    protected static final String VALUE = "value";

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(Field field) {
            return ElasticEntity.class.isAssignableFrom(field.getDeclaringClass())
                   && StringMap.class.equals(field.getType());
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

            propertyConsumer.accept(new ESStringMapProperty(descriptor, accessPath, field));
        }
    }

    ESStringMapProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object transformFromDatasource(Value object) {
        Map<Object, Object> result = new HashMap<>();
        Object value = object.get();
        if (value instanceof Collection) {
            ((Collection<Map<?, ?>>) value).forEach(entry -> result.put(entry.get(KEY), entry.get(VALUE)));
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object transformToDatasource(Object object) {
        return ((Map<?, ?>) object).entrySet()
                                   .stream()
                                   .map(e -> new JSONObject().fluentPut(KEY, e.getKey()).fluentPut(VALUE, e.getValue()))
                                   .collect(Collectors.toList());
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
        properties.put(KEY, new JSONObject().fluentPut(IndexMappings.MAPPING_TYPE, IndexMappings.MAPPING_TYPE_KEWORD));
        properties.put(VALUE,
                       new JSONObject().fluentPut(IndexMappings.MAPPING_TYPE, IndexMappings.MAPPING_TYPE_KEWORD));
        description.put("properties", properties);
        description.put("dynamic", false);
    }
}
