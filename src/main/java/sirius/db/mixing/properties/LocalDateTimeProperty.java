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
import sirius.kernel.nls.NLS;

import java.lang.reflect.Field;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.function.Consumer;

/**
 * Represents an {@link LocalDateTime} field within a {@link sirius.db.mixing.Mixable}.
 */
public class LocalDateTimeProperty extends Property {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(Field field) {
            return LocalDateTime.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new LocalDateTimeProperty(descriptor, accessPath, field));
        }
    }

    LocalDateTimeProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    protected Object transformValue(Value value) {
        return NLS.parseUserString(LocalDateTime.class, value.asString());
    }

    @Override
    protected int getSQLType() {
        return Types.BIGINT;
    }

    @Override
    protected Object transformFromColumn(Object object) {
        if (object == null) {
            return null;
        }
        return Instant.ofEpochMilli((long) object).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    @Override
    protected Object transformToColumn(Object object) {
        return object == null ?
               null :
               ((LocalDateTime) object).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
}
