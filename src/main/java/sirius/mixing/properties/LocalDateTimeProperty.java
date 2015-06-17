/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.properties;

import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.nls.NLS;
import sirius.mixing.AccessPath;
import sirius.mixing.EntityDescriptor;
import sirius.mixing.Property;
import sirius.mixing.PropertyFactory;

import java.lang.reflect.Field;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.function.Consumer;

/**
 * Created by aha on 15.04.15.
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

    public LocalDateTimeProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
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
        return Instant.ofEpochMilli((long) object).atOffset(ZoneOffset.UTC).toLocalDateTime();
    }

    @Override
    protected Object transformToColumn(Object object) {
        return object == null ? null : ((LocalDateTime) object).atOffset(ZoneOffset.UTC).toInstant().toEpochMilli();
    }
}
