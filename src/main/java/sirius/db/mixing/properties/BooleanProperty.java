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
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.function.Consumer;

/**
 * Represents a {@link Boolean} field within a {@link sirius.db.mixing.Mixable}.
 */
public class BooleanProperty extends Property {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(Field field) {
            return Boolean.class.equals(field.getType()) || boolean.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new BooleanProperty(descriptor, accessPath, field));
        }
    }

    BooleanProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
        this.nullable = false;
    }

    @Override
    protected Object transformValue(Value value) {
        return value.asBoolean(false);
    }

    @Override
    protected Object transformFromColumn(Object object) {
        if (object instanceof Boolean) {
            return object;
        }

        return ((Integer) object) != 0;
    }

    @Override
    protected Object transformToColumn(Object object) {
        return ((Boolean) object) ? 1 : 0;
    }

    @Override
    protected int getSQLType() {
        return Types.TINYINT;
    }
}
