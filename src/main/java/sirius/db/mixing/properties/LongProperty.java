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
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.function.Consumer;

/**
 * Represents an {@link Long} field within a {@link sirius.db.mixing.Mixable}.
 */
public class LongProperty extends Property {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(Field field) {
            return Long.class.equals(field.getType()) || long.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new LongProperty(descriptor, accessPath, field));
        }
    }

    LongProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    public Object transformValue(Value value) {
        if (value.isFilled()) {
            Long result = value.getLong();
            if (result == null) {
                throw illegalFieldValue(value);
            }
            return result;
        } else {
            if (this.isNullable() || Strings.isEmpty(defaultValue)) {
                return null;
            } else {
                return Value.of(defaultValue).getLong();
            }
        }
    }

    @Override
    protected int getSQLType() {
        return Types.BIGINT;
    }
}
