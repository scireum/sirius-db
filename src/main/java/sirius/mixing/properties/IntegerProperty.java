/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.properties;

import sirius.kernel.di.std.Register;
import sirius.mixing.AccessPath;
import sirius.mixing.EntityDescriptor;
import sirius.mixing.Property;
import sirius.mixing.PropertyFactory;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.function.Consumer;

/**
 * Created by aha on 15.04.15.
 */
public class IntegerProperty extends Property {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(Field field) {
            return Integer.class.equals(field.getType()) || int.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new IntegerProperty(descriptor, accessPath, field));
        }

    }


    public IntegerProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    protected int getSQLType() {
        return Types.INTEGER;
    }
}
