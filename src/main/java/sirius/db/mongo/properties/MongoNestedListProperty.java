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
import sirius.db.mixing.Nested;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.properties.BaseNestedListProperty;
import sirius.db.mixing.types.NestedList;
import sirius.db.mixing.types.StringList;
import sirius.db.mongo.Doc;
import sirius.db.mongo.Mongo;
import sirius.db.mongo.MongoEntity;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Represents an {@link NestedList} field within a {@link MongoEntity}.
 */
public class MongoNestedListProperty extends BaseNestedListProperty {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(Field field) {
            return MongoEntity.class.isAssignableFrom(field.getDeclaringClass())
                   && NestedList.class.equals(field.getType());
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

            propertyConsumer.accept(new MongoNestedListProperty(descriptor, accessPath, field));
        }
    }

    MongoNestedListProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object transformToDatasource(Object object) {
        List<Document> result = new ArrayList<>();

        for (Nested obj : (List<Nested>) object) {
            Document inner = new Document();
            for (Property property : getNestedDescriptor().getProperties()) {
                inner.put(property.getPropertyName(), property.getValue(obj));
            }
            result.add(inner);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object transformFromDatasource(Value object) {
        List<Nested> result = new ArrayList<>();
        Object obj = object.get();
        if (obj instanceof List) {
            for (Document doc : (List<Document>) obj) {
                try {
                    Doc innerDoc = new Doc(doc);
                    result.add((Nested) getNestedDescriptor().make(null, innerDoc::get));
                } catch (Exception e) {
                    throw Exceptions.handle(Mongo.LOG, e);
                }
            }
        }

        return result;
    }
}
