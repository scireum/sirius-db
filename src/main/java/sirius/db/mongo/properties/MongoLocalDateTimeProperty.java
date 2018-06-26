/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties;

import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mongo.MongoEntity;
import sirius.db.mongo.QueryBuilder;
import sirius.db.mongo.constraints.MongoFilterFactory;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.function.Consumer;

/**
 * Represents a timestamp property which contains a date along with a time value. This is used to represents fields of
 * type {@link LocalDateTime}.
 */
public class MongoLocalDateTimeProperty extends Property {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return MongoEntity.class.isAssignableFrom(descriptor.getType())
                   && LocalDateTime.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new MongoLocalDateTimeProperty(descriptor, accessPath, field));
        }
    }

    protected MongoLocalDateTimeProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    public Object transformValue(Value value) {
        return value.asLocalDateTime(null);
    }

    @Override
    protected Object transformFromDatasource(Value object) {
        return object.asLocalDateTime(null);
    }

    @Override
    protected Object transformToDatasource(Object object) {
        if (!(object instanceof LocalDateTime)) {
            return null;
        }

        return QueryBuilder.FILTERS.transform(object);
    }
}
