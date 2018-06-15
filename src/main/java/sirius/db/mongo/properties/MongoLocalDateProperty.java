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
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;

import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.function.Consumer;

/**
 * Represents a date property which contains no associated time value. This is used to represents fields of type
 * {@link LocalDate}.
 */
public class MongoLocalDateProperty extends Property {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(Field field) {
            return MongoEntity.class.isAssignableFrom(field.getDeclaringClass())
                   && LocalDate.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new MongoLocalDateProperty(descriptor, accessPath, field));
        }
    }

    protected MongoLocalDateProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    public Object transformValue(Value value) {
        return value.asLocalDate(null);
    }

    @Override
    protected Object transformFromDatasource(Value object) {
        return object.asLocalDate(null);
    }

    @Override
    protected Object transformToDatasource(Object object) {
        if (!(object instanceof LocalDate)) {
            return null;
        }

        return QueryBuilder.transformValue(object);
    }
}
