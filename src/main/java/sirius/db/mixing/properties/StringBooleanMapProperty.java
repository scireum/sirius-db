/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.bson.Document;
import sirius.db.es.ESPropertyInfo;
import sirius.db.es.IndexMappings;
import sirius.db.es.annotations.ESOption;
import sirius.db.es.annotations.IndexMode;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.types.StringBooleanMap;
import sirius.kernel.commons.Json;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Represents an {@link StringBooleanMap} field within a {@link Mixable}.
 */
public class StringBooleanMapProperty extends BaseMapProperty implements ESPropertyInfo {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return StringBooleanMap.class.equals(field.getType());
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

            propertyConsumer.accept(new StringBooleanMapProperty(descriptor, accessPath, field));
        }
    }

    StringBooleanMapProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object transformToMongo(Object object) {
        if (object instanceof Document) {
            return object;
        }

        Document doc = new Document();
        doc.putAll((Map<String, Boolean>) object);

        return doc;
    }

    @Override
    protected Object transformToElastic(Object object) {
        return ((Map<?, ?>) object).entrySet()
                                   .stream()
                                   .map(e -> Json.createObject()
                                                 .putPOJO(StringMapProperty.KEY, e.getKey())
                                                 .putPOJO(StringMapProperty.VALUE, e.getValue()))
                                   .toList();
    }

    @Override
    protected Object transformFromMongo(Value object) {
        return object.get();
    }

    @Override
    public void describeProperty(ObjectNode description) {
        ESOption indexed = Optional.ofNullable(getClass().getAnnotation(IndexMode.class))
                                   .map(IndexMode::indexed)
                                   .orElse(ESOption.ES_DEFAULT);

        if (ESOption.FALSE == indexed) {
            description.put(IndexMappings.MAPPING_TYPE, "object");
        } else {
            description.put(IndexMappings.MAPPING_TYPE, "nested");
        }
        transferOption(IndexMappings.MAPPING_STORED, getAnnotation(IndexMode.class), IndexMode::stored, description);

        ObjectNode properties = Json.createObject();
        properties.set(StringMapProperty.KEY,
                       Json.createObject().put(IndexMappings.MAPPING_TYPE, IndexMappings.MAPPING_TYPE_KEWORD));
        properties.set(StringMapProperty.VALUE, Json.createObject().put(IndexMappings.MAPPING_TYPE, "boolean"));
        description.set("properties", properties);
    }
}
