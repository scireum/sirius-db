/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Property;
import sirius.db.mixing.types.SafeMap;
import sirius.kernel.commons.Value;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a {@link SafeMap} field within a {@link Mixable}.
 */
public abstract class BaseMapProperty extends Property {

    /**
     * Contains the name of the field used to store the map key
     */
    public static final String KEY = "key";

    /**
     * Contains the name of the field used to store the map value
     */
    public static final String VALUE = "value";

    protected BaseMapProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    protected Object getValueFromField(Object target) {
        return ((SafeMap<?, ?>) super.getValueFromField(target)).data();
    }

    @Override
    public Object getValueAsCopy(Object entity) {
        Object target = accessPath.apply(entity);
        return ((SafeMap<?, ?>) super.getValueFromField(target)).copyMap();
    }

    @Override
    public Object transformValue(Value value) {
        if (value.isEmptyString()) {
            return null;
        }

        return value.get();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void setValueToField(Object value, Object target) {
        ((SafeMap<Object, Object>) super.getValueFromField(target)).setData((Map<Object, Object>) value);
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
