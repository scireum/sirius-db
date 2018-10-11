/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import com.alibaba.fastjson.JSONObject;
import org.bson.Document;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.types.StringBooleanMap;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Represents an {@link StringBooleanMap} field within a {@link Mixable}.
 */
public class StringBooleanMapProperty extends BaseMapProperty {

    /**
     * Contains the name of the field used to store the map key
     */
    public static final String KEY = "key";

    /**
     * Contains the name of the field used to store the map value
     */
    public static final String VALUE = "value";

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return StringBooleanMap.class.equals(field.getType());
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

            propertyConsumer.accept(new StringBooleanMapProperty(descriptor, accessPath, field));
        }
    }

    StringBooleanMapProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object transformToMongo(Object object) {
        if (object instanceof Document) {
            return object;
        }
        
        Document doc = new Document();
        doc.putAll((Map<String, Boolean>) object);

        return doc;
    }

    @Override
    protected Object transformToElastic(Object object) {
        return ((Map<?, ?>) object).entrySet()
                                   .stream()
                                   .map(e -> new JSONObject().fluentPut(KEY, e.getKey()).fluentPut(VALUE, e.getValue()))
                                   .collect(Collectors.toList());
    }

    @Override
    protected Object transformFromMongo(Value object) {
        return object.get();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object transformFromElastic(Value object) {
        Map<Object, Object> result = new HashMap<>();
        Object value = object.get();
        if (value instanceof Collection) {
            ((Collection<Map<?, ?>>) value).forEach(entry -> result.put(entry.get(KEY), entry.get(VALUE)));
        }
        return result;
    }
}
