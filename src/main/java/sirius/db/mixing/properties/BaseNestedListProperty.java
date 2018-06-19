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
import sirius.db.mixing.Nested;
import sirius.db.mixing.Property;
import sirius.db.mixing.types.NestedList;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Represents a {@link NestedList} field within a {@link Mixable}.
 */
public class BaseNestedListProperty extends Property {

    @Part
    private static Mixing mixing;
    private EntityDescriptor nestedDescriptor;

    protected BaseNestedListProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    protected NestedList<?> getNestedList(Object entity) {
        try {
            return (NestedList<?>) super.getValueFromField(entity);
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
        return getNestedList(target).data();
    }

    @Override
    public Object getValueAsCopy(Object entity) {
        return getNestedList(entity).copyList();
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
        ((NestedList<Nested>) getNestedList(target)).setData((List<Nested>) value);
    }

    @Override
    protected void onBeforeSaveChecks(Object entity) {
        List<?> list = (List<?>) getValueFromField(entity);
        list.forEach(value -> getNestedDescriptor().beforeSave(value));
    }

    protected EntityDescriptor getNestedDescriptor() {
        if (nestedDescriptor == null) {
            nestedDescriptor = mixing.getDescriptor(getNestedList(descriptor.getReferenceInstance()).getNestedType());
        }

        return nestedDescriptor;
    }
}
