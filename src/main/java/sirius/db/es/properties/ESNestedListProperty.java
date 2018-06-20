/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties;

import com.alibaba.fastjson.JSONObject;
import sirius.db.es.ESPropertyInfo;
import sirius.db.es.Elastic;
import sirius.db.es.ElasticEntity;
import sirius.db.es.IndexMappings;
import sirius.db.es.annotations.ESOption;
import sirius.db.es.annotations.IndexMode;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Nested;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.properties.BaseNestedListProperty;
import sirius.db.mixing.types.NestedList;
import sirius.db.mongo.Mongo;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Represents an {@link NestedList} field within an {@link ElasticEntity}.
 * <p>
 * Creates an appropriate mapping for the nested objects. By default the inner type
 * is <tt>nested</tt> to permit proper queries. Use {@link IndexMode#indexed()} (set to FALSE) to
 * set the type to <tt>object</tt>.
 */
public class ESNestedListProperty extends BaseNestedListProperty implements ESPropertyInfo {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return ElasticEntity.class.isAssignableFrom(descriptor.getType())
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

            propertyConsumer.accept(new ESNestedListProperty(descriptor, accessPath, field));
        }
    }

    ESNestedListProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object transformToDatasource(Object object) {
        List<JSONObject> result = new ArrayList<>();

        for (Nested obj : (List<Nested>) object) {
            JSONObject inner = new JSONObject();
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
            for (JSONObject doc : (List<JSONObject>) obj) {
                try {
                    result.add((Nested) getNestedDescriptor().make(null, key -> Value.of(doc.get(key))));
                } catch (Exception e) {
                    throw Exceptions.handle(Mongo.LOG, e);
                }
            }
        }

        return result;
    }

    @Override
    public void describeProperty(JSONObject description) {
        ESOption indexed = Optional.ofNullable(getClass().getAnnotation(IndexMode.class))
                                   .map(IndexMode::indexed)
                                   .orElse(ESOption.ES_DEFAULT);

        if (ESOption.FALSE == indexed) {
            description.put(IndexMappings.MAPPING_TYPE, "object");
        } else {
            description.put(IndexMappings.MAPPING_TYPE, "nested");
        }
        transferOption(IndexMappings.MAPPING_STORED, IndexMode::stored, ESOption.ES_DEFAULT, description);

        JSONObject properties = new JSONObject();
        for (Property property : getNestedDescriptor().getProperties()) {
            if (!(property instanceof ESPropertyInfo)) {
                Exceptions.handle()
                          .to(Elastic.LOG)
                          .withSystemErrorMessage("The nested %s in %s of %s (%s) contains an unmappable property"
                                                  + " %s - ESPropertyInfo is not available!",
                                                  getNestedDescriptor().getType().getName(),
                                                  getName(),
                                                  getDescriptor().getType().getName(),
                                                  getDescriptor().getRelationName(),
                                                  property.getName())
                          .handle();
            } else {
                JSONObject propertyInfo = new JSONObject();
                ((ESPropertyInfo) property).describeProperty(propertyInfo);
                properties.put(property.getPropertyName(), propertyInfo);
            }
        }
        description.put("properties", properties);
        description.put("dynamic", false);
    }
}
