/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import sirius.db.mixing.AccessPath;
import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Property;
import sirius.db.mixing.types.SafeMap;
import sirius.kernel.commons.Value;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Represents an {@link SafeMap} field within a {@link Mixable}.
 */
public abstract class BaseMapProperty extends Property {

    protected BaseMapProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    protected Object getValueFromField(Object target) {
        return ((SafeMap<?, ?>) super.getValueFromField(target)).data();
    }

    @Override
    public Object getValueAsCopy(Object entity) {
        return ((SafeMap<?, ?>) super.getValueFromField(entity)).copyValue();
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
        ((SafeMap<Object, Object>) super.getValueFromField(target)).setData((Map<Object, Object>) value);
    }
}
