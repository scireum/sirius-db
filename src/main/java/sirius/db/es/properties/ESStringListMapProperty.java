/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties;

import sirius.db.es.ElasticEntity;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.types.StringListMap;
import sirius.kernel.di.std.Register;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.function.Consumer;

/**
 * Represents an {@link StringListMap} field within an {@link ElasticEntity}.
 */
public class ESStringListMapProperty extends ESStringMapProperty {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(Field field) {
            return ElasticEntity.class.isAssignableFrom(field.getDeclaringClass())
                   && StringListMap.class.equals(field.getType());
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

            propertyConsumer.accept(new ESStringListMapProperty(descriptor, accessPath, field));
        }
    }

    ESStringListMapProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }
}
