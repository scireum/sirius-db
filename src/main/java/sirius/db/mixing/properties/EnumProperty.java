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
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.annotations.Ordinal;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.function.Consumer;

/**
 * Represents an {@link Enum} field within a {@link sirius.db.mixing.Mixable}.
 */
public class EnumProperty extends Property {

    private boolean ordinal;

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(Field field) {
            return field.getType().isEnum();
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new EnumProperty(descriptor, accessPath, field));
        }
    }

    EnumProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
        this.ordinal = field.isAnnotationPresent(Ordinal.class);
    }

    @SuppressWarnings({"unchecked", "raw", "rawtypes"})
    @Override
    protected Object transformValue(Value value) {
        return value.asEnum((Class<Enum>) field.getType());
    }

    @Override
    protected int getSQLType() {
        return ordinal ? Types.INTEGER : Types.CHAR;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void determineLengths() {
        for (Enum<?> e : ((Class<Enum<?>>) field.getType()).getEnumConstants()) {
            this.length = Math.max(this.length, e.name().length());
        }
    }

    @SuppressWarnings({"unchecked", "raw", "rawtypes"})
    @Override
    protected Object transformFromColumn(Object object) {
        if (object == null) {
            return null;
        }
        if (ordinal) {
            Object[] values = field.getType().getEnumConstants();
            int index = Value.of(object).asInt(0);
            index = Math.min(index, values.length - 1);
            return values[index];
        }
        return Value.of(object).asEnum((Class<Enum>) field.getType());
    }

    @Override
    protected Object transformToColumn(Object object) {
        if (object == null) {
            return null;
        }
        return ordinal ? ((Enum<?>) object).ordinal() : ((Enum<?>) object).name();
    }
}
