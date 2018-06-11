/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.properties;

import sirius.db.jdbc.Databases;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.jdbc.SQLEntity;
import sirius.db.jdbc.schema.SQLPropertyInfo;
import sirius.db.jdbc.schema.Table;
import sirius.db.jdbc.schema.TableColumn;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.nls.NLS;

import java.lang.reflect.Field;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.function.Consumer;

/**
 * Represents an {@link LocalDateTime} field within a {@link Mixable}.
 */
public class JDBCLocalDateTimeProperty extends Property implements SQLPropertyInfo {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(Field field) {
            return SQLEntity.class.isAssignableFrom(field.getDeclaringClass())
                   && LocalDateTime.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new JDBCLocalDateTimeProperty(descriptor, accessPath, field));
        }
    }

    JDBCLocalDateTimeProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    public Object transformValue(Value value) {
        return NLS.parseUserString(LocalDateTime.class, value.asString());
    }

    @Override
    protected Object transformFromDatasource(Value data) {
        Object object = data.get();
        if (object == null) {
            return null;
        }
        return Databases.decodeLocalDateTime((long) object);
    }

    @Override
    protected Object transformToDatasource(Object object) {
        if (object == null) {
            return null;
        }
        return Databases.encodeLocalDateTime((LocalDateTime) object);
    }

    @Override
    public void contributeToTable(Table table) {
        table.getColumns().add(new TableColumn(this, Types.BIGINT));
    }
}
