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
import java.sql.Date;
import java.sql.Types;
import java.time.LocalDate;
import java.util.function.Consumer;

/**
 * Represents an {@link LocalDate} field within a {@link sirius.db.mixing.Mixable}.
 */
public class LocalDateProperty extends Property {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(Field field) {
            return LocalDate.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new LocalDateProperty(descriptor, accessPath, field));
        }
    }

    LocalDateProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    protected Object transformValue(Value value) {
        return NLS.parseUserString(LocalDate.class, value.asString());
    }

    @Override
    protected int getSQLType() {
        return Types.DATE;
    }

    @Override
    protected Object transformFromColumn(Object object) {
        if (object == null) {
            return null;
        }
        return ((Date) object).toLocalDate();
    }

    @Override
    protected Object transformToColumn(Object object) {
        return object == null ? null : Date.valueOf((LocalDate) object);
    }
}
