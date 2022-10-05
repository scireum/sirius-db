/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import sirius.db.jdbc.schema.SQLPropertyInfo;
import sirius.db.jdbc.schema.Table;
import sirius.db.jdbc.schema.TableColumn;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.nls.NLS;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.function.Consumer;

/**
 * Represents an {@link Instant} field within a {@link Mixable}.
 */
public class InstantProperty extends Property implements SQLPropertyInfo {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return Instant.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new InstantProperty(descriptor, accessPath, field));
        }
    }

    InstantProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    public Object transformValue(Value value) {
        return NLS.parseUserString(LocalDateTime.class, value.asString()).atZone(ZoneId.systemDefault()).toInstant();
    }

    @Override
    protected Object transformFromJDBC(Value data) {
        Object object = data.get();
        if (object == null) {
            return null;
        }
        if (data.is(LocalDateTime.class)) {
            return ((LocalDateTime) (data.get())).truncatedTo(ChronoUnit.MILLIS)
                                                 .atZone(ZoneId.systemDefault())
                                                 .toInstant();
        }

        return ((Timestamp) object).toInstant();
    }

    @Override
    protected Object transformToJDBC(Object object) {
        if (object == null) {
            return null;
        }
        return new Timestamp(((Instant) object).toEpochMilli());
    }

    @Override
    public void contributeToTable(Table table) {
        table.getColumns().add(new TableColumn(this, Types.TIMESTAMP));
    }
}
