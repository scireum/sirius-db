/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import com.alibaba.fastjson2.JSONObject;
import org.bson.Document;
import sirius.db.es.ESPropertyInfo;
import sirius.db.es.Elastic;
import sirius.db.es.IndexMappings;
import sirius.db.es.annotations.ESOption;
import sirius.db.es.annotations.IndexMode;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.types.StringLocalDateTimeMap;
import sirius.db.mongo.Mongo;
import sirius.db.mongo.QueryBuilder;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Represents an {@link StringLocalDateTimeMap} field within a {@link Mixable}.
 */
public class StringLocalDateTimeMapProperty extends BaseMapProperty implements ESPropertyInfo {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return StringLocalDateTimeMap.class.equals(field.getType());
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

            propertyConsumer.accept(new StringLocalDateTimeMapProperty(descriptor, accessPath, field));
        }
    }

    StringLocalDateTimeMapProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object transformToMongo(Object object) {
        Document doc = new Document();
        ((Map<String, StringLocalDateTimeMap>) object).forEach((k, v) -> doc.put(k, QueryBuilder.FILTERS.transform(v)));
        return doc;
    }

    @Override
    protected Object transformFromMongo(Value object) {
        Map<String, LocalDateTime> result = new LinkedHashMap<>();
        Object obj = object.get();
        if (obj instanceof Document document) {
            for (Map.Entry<String, Object> entry : document.entrySet()) {
                try {
                    result.put(entry.getKey(),
                               Value.of(entry.getValue()).asLocalDateTime(null).truncatedTo(ChronoUnit.MILLIS));
                } catch (Exception e) {
                    throw Exceptions.handle(Mongo.LOG, e);
                }
            }
        }

        return result;
    }

    @Override
    protected Object transformToElastic(Object object) {
        return ((Map<?, ?>) object).entrySet()
                                   .stream()
                                   .map(e -> new JSONObject().fluentPut(StringMapProperty.KEY, e.getKey())
                                                             .fluentPut(StringMapProperty.VALUE,
                                                                        Elastic.FILTERS.transform(e.getValue())))
                                   .toList();
    }

    private LocalDateTime readDate(String date) {
        if (Strings.isEmpty(date)) {
            return null;
        }

        try {
            return LocalDateTime.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(date))
                                .truncatedTo(ChronoUnit.MILLIS);
        } catch (DateTimeParseException e) {
            return LocalDateTime.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(date)).truncatedTo(ChronoUnit.MILLIS);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object transformFromElastic(Value object) {
        Map<String, Object> result = new HashMap<>();
        Object value = object.get();
        if (value instanceof Collection) {
            ((Collection<Map<String, Object>>) value).forEach(entry -> result.put((String) entry.get(StringMapProperty.KEY),
                                                                                  readDate((String) entry.get(
                                                                                          StringMapProperty.VALUE))));
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
        transferOption(IndexMappings.MAPPING_STORED, getAnnotation(IndexMode.class), IndexMode::stored, description);

        JSONObject properties = new JSONObject();
        properties.put(StringMapProperty.KEY,
                       new JSONObject().fluentPut(IndexMappings.MAPPING_TYPE, IndexMappings.MAPPING_TYPE_KEWORD));
        properties.put(StringMapProperty.VALUE, new JSONObject().fluentPut(IndexMappings.MAPPING_TYPE, "date"));
        description.put("properties", properties);
    }
}
