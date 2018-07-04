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
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.types.SafeMap;
import sirius.kernel.commons.Value;
import sirius.kernel.health.Exceptions;

import java.lang.reflect.Field;
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

    protected SafeMap<?, ?> getMap(Object entity) {
        try {
            return (SafeMap<?, ?>) super.getValueFromField(entity);
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .error(e)
                            .withSystemErrorMessage(
                                    "Unable to obtain EntityRef object from entity ref field ('%s' in '%s'): %s (%s)",
                                    getName(),
                                    descriptor.getType().getName())
                            .handle();
        }
    }

    @Override
    protected Object getValueFromField(Object target) {
        return getMap(target).data();
    }

    @Override
    public Object getValueAsCopy(Object entity) {
        return getMap(entity).copyMap();
    }

    @SuppressWarnings("unchecked")
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
        ((SafeMap<Object, Object>) getMap(target)).setData((Map<Object, Object>) value);
    }
}
