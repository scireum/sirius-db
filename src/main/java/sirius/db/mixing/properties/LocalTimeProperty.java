/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Property;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.nls.NLS;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.PropertyFactory;

import java.lang.reflect.Field;
import java.sql.Time;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.function.Consumer;

/**
 * Created by aha on 15.04.15.
 */
public class LocalTimeProperty extends Property {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(Field field) {
            return LocalTime.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new LocalTimeProperty(descriptor, accessPath, field));
        }
    }

    public LocalTimeProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    protected Object transformValue(Value value) {
        return NLS.parseUserString(LocalTime.class, value.asString());
    }

    @Override
    protected int getSQLType() {
        return Types.TIME;
    }

    @Override
    protected Object transformFromColumn(Object object) {
        if (object == null) {
            return null;
        }
        return Instant.ofEpochMilli(((Time) object).getTime()).atZone(ZoneId.systemDefault()).toLocalTime();
    }

    @Override
    protected Object transformToColumn(Object object) {
        return object == null ?
               null :
               new Time(((LocalTime) object).atDate(LocalDate.of(1970, 01, 01))
                                            .atZone(ZoneId.systemDefault())
                                            .toInstant()
                                            .toEpochMilli());
    }
}
