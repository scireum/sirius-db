/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.properties;

import sirius.kernel.di.std.Register;

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
        public void create(AccessPath accessPath, Field field, Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new IntegerProperty(accessPath, field));
        }

    }


    public IntegerProperty(AccessPath accessPath, Field field) {
        super(accessPath, field);
    }

    @Override
    protected int getSQLType() {
        return Types.INTEGER;
    }
}
