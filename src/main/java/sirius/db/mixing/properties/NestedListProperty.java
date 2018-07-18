/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import com.alibaba.fastjson.JSONObject;
import org.bson.Document;
import sirius.db.es.ESPropertyInfo;
import sirius.db.es.Elastic;
import sirius.db.es.IndexMappings;
import sirius.db.es.annotations.ESOption;
import sirius.db.es.annotations.IndexMode;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Nested;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.types.NestedList;
import sirius.db.mongo.Doc;
import sirius.db.mongo.Mango;
import sirius.db.mongo.Mongo;
import sirius.db.mongo.MongoEntity;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Represents an {@link NestedList} field within a {@link MongoEntity}.
 */
public class NestedListProperty extends Property implements ESPropertyInfo {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return NestedList.class.equals(field.getType());
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

            propertyConsumer.accept(new NestedListProperty(descriptor, accessPath, field));
        }
    }

    @Part
    private static Mixing mixing;
    private EntityDescriptor nestedDescriptor;

    protected NestedListProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    protected NestedList<?> getNestedList(Object entity) {
        try {
            return (NestedList<?>) super.getValueFromField(entity);
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .error(e)
                            .withSystemErrorMessage(
                                    "Unable to obtain EntityRef object from entity ref field ('%s' in '%s'): %s (%s)",
                                    getName(),
                                    descriptor.getType().getName())
                            .handle();
        }
    }

    @Override
    protected Object getValueFromField(Object target) {
        return getNestedList(target).data();
    }

    @Override
    public Object getValueAsCopy(Object entity) {
        return getNestedList(entity).copyList();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object transformValue(Value value) {
        if (value.isEmptyString()) {
            return null;
        }

        return value.get();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void setValueToField(Object value, Object target) {
        ((NestedList<Nested>) getNestedList(target)).setData((List<Nested>) value);
    }

    @Override
    protected void onBeforeSaveChecks(Object entity) {
        List<?> list = (List<?>) getValueFromField(entity);
        list.forEach(value -> getNestedDescriptor().beforeSave(value));
    }

    protected EntityDescriptor getNestedDescriptor() {
        if (nestedDescriptor == null) {
            nestedDescriptor = mixing.getDescriptor(getNestedList(descriptor.getReferenceInstance()).getNestedType());
        }

        return nestedDescriptor;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object transformToDatasource(Class<? extends BaseMapper<?, ?, ?>> mapperType, Object object) {
        if (mapperType == Mango.class) {
            List<Document> result = new ArrayList<>();

            for (Nested obj : (List<Nested>) object) {
                Document inner = new Document();
                for (Property property : getNestedDescriptor().getProperties()) {
                    inner.put(property.getPropertyName(), property.getValueForDatasource(Mango.class, obj));
                }
                result.add(inner);
            }

            return result;
        } else if (mapperType == Elastic.class) {
            List<JSONObject> result = new ArrayList<>();

            for (Nested obj : (List<Nested>) object) {
                JSONObject inner = new JSONObject();
                for (Property property : getNestedDescriptor().getProperties()) {
                    inner.put(property.getPropertyName(), property.getValueForDatasource(Elastic.class, obj));
                }
                result.add(inner);
            }

            return result;
        } else {
            throw new UnsupportedOperationException("NestedListProperty only supports Mango or Elastic as mapper!");
        }
    }

    @Override
    protected Object transformFromDatasource(Class<? extends BaseMapper<?, ?, ?>> mapperType, Value object) {
        if (mapperType == Mango.class) {
            return transformFromMango(object);
        } else if (mapperType == Elastic.class) {
            return transfromFromElastic(object);
        } else {
            throw new UnsupportedOperationException("NestedListProperty only supports Mango or Elastic as mapper!");
        }
    }

    @SuppressWarnings("unchecked")
    private Object transfromFromElastic(Value object) {
        List<Nested> result = new ArrayList<>();
        Object obj = object.get();
        if (obj instanceof List) {
            for (JSONObject doc : (List<JSONObject>) obj) {
                try {
                    result.add((Nested) getNestedDescriptor().make(Elastic.class, null, key -> Value.of(doc.get(key))));
                } catch (Exception e) {
                    throw Exceptions.handle(Mongo.LOG, e);
                }
            }
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private Object transformFromMango(Value object) {
        List<Nested> result = new ArrayList<>();
        Object obj = object.get();
        if (obj instanceof List) {
            for (Document doc : (List<Document>) obj) {
                try {
                    Doc innerDoc = new Doc(doc);
                    result.add((Nested) getNestedDescriptor().make(Mango.class, null, innerDoc::get));
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

        transferOption(IndexMappings.MAPPING_STORED,
                       getAnnotation(IndexMode.class),
                       IndexMode::stored,
                       description);

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
