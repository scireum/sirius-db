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
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.types.NestedList;
import sirius.db.mixing.types.StringList;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.function.Consumer;

/**
 * Represents an {@link StringList} field within a {@link Mixable}.
 */
public class NestedListProperty extends Property implements ESPropertyInfo {

    @Part
    private static Mixing mixing;
    private EntityDescriptor nestedDescriptor;

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(Field field) {
            return NestedList.class.equals(field.getType());
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

            propertyConsumer.accept(new NestedListProperty(descriptor, accessPath, field));
        }
    }

    NestedListProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    protected Object getValueFromField(Object target) {
        return ((NestedList<?>) super.getValueFromField(target)).data();
    }

    @Override
    public Object getValueAsCopy(Object entity) {
        return ((NestedList<?>) super.getValueFromField(entity)).copyValue();
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
        ((StringList) super.getValueFromField(target)).setData((List<String>) value);
    }

    @Override
    protected void onBeforeSaveChecks(Object entity) {
        NestedList<?> list = (NestedList<?>) getValueFromField(entity);
        list.forEach(value -> getNestedDescriptor().beforeSave(value));
    }

    private EntityDescriptor getNestedDescriptor() {
        if (nestedDescriptor == null) {
            nestedDescriptor =
                    mixing.getDescriptor(((NestedList<?>) super.getValueFromField(descriptor.getReferenceInstance())).getNestedType());
        }

        return nestedDescriptor;
    }

    @Override
    public void describeProperty(JSONObject description) {

        //TODO
        description.put("type", "keyword");
    }
}
