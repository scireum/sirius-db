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

    @Override
    protected Object getValueFromField(Object target) {
        return ((NestedList<?>) super.getValueFromField(target)).data();
    }

    @Override
    public Object getValueAsCopy(Object entity) {
        return ((NestedList<?>) super.getValueFromField(entity)).copyList();
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
        ((NestedList<Nested>) super.getValueFromField(target)).setData((List<Nested>) value);
    }

    @Override
    protected void onBeforeSaveChecks(Object entity) {
        List<?> list = (List<?>) getValueFromField(entity);
        list.forEach(value -> getNestedDescriptor().beforeSave(value));
    }

    protected EntityDescriptor getNestedDescriptor() {
        if (nestedDescriptor == null) {
            nestedDescriptor =
                    mixing.getDescriptor(((NestedList<?>) super.getValueFromField(descriptor.getReferenceInstance())).getNestedType());
        }

        return nestedDescriptor;
    }

}
