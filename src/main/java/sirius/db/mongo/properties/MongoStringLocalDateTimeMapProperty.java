/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties;

import org.bson.Document;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.properties.BaseMapProperty;
import sirius.db.mongo.MongoEntity;
import sirius.db.mongo.QueryBuilder;
import sirius.db.mongo.types.StringLocalDateTimeMap;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Represents an {@link StringLocalDateTimeMap} field within a {@link Mixable}.
 */
public class MongoStringLocalDateTimeMapProperty extends BaseMapProperty {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(Field field) {
            return MongoEntity.class.isAssignableFrom(field.getDeclaringClass()) && StringLocalDateTimeMap.class.equals(
                    field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            if (!Modifier.isFinal(field.getModifiers())) {
                Mixing.LOG.WARN("Field %s in %s is not final! This will probably result in errors.",
                                field.getName(),
                                field.getDeclaringClass().getName());
            }

            propertyConsumer.accept(new MongoStringLocalDateTimeMapProperty(descriptor, accessPath, field));
        }
    }

    MongoStringLocalDateTimeMapProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object transformToDatasource(Object object) {
        Document doc = new Document();
        ((Map<String, StringLocalDateTimeMap>) object).forEach((k, v) -> doc.put(k, QueryBuilder.transformValue(v)));
        return doc;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void setValueToField(Object value, Object target) {
        try {
            StringLocalDateTimeMap map = (StringLocalDateTimeMap) field.get(target);
            map.clear();
            if (value instanceof Document) {
                ((Document) value).forEach((k, v) -> map.put(k, Value.of(v).asLocalDateTime(null)));
            }
        } catch (IllegalAccessException e) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .error(e)
                            .withSystemErrorMessage("Cannot read property '%s' (from '%s'): %s (%s)",
                                                    getName(),
                                                    getDefinition())
                            .handle();
        }
    }
}
