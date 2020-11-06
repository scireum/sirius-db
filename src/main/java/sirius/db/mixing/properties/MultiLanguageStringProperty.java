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
import sirius.db.es.IndexMappings;
import sirius.db.es.annotations.IndexMode;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.types.MultiLanguageString;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Represents a {@link MultiLanguageString} field in a {@link sirius.db.mongo.MongoEntity}.
 * <p>
 * Multi-Language Strings are stored as a list of nested objects which contain a
 * <tt>lang</tt> and a <tt>text</tt> property.
 */
public class MultiLanguageStringProperty extends BaseMapProperty implements ESPropertyInfo {

    private static final String LANGUAGE_PROPERTY = "lang";
    private static final String TEXT_PROPERTY = "text";

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return MultiLanguageString.class.equals(field.getType());
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

            propertyConsumer.accept(new MultiLanguageStringProperty(descriptor, accessPath, field));
        }
    }

    MultiLanguageStringProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    protected void onBeforeSaveChecks(Object entity) {
        MultiLanguageString multiLanguageString = getMultiLanguageString(entity);
        if (!multiLanguageString.getValidLanguages().isEmpty()) {
            multiLanguageString.data()
                               .entrySet()
                               .stream()
                               .filter(entry -> !Strings.areEqual(entry.getKey(), MultiLanguageString.FALLBACK_KEY))
                               .filter(entry -> !multiLanguageString.getValidLanguages().contains(entry.getKey()))
                               .findAny()
                               .ifPresent(entry -> {
                                   throw Exceptions.createHandled()
                                                   .withNLSKey("MultiLanguageString.invalidLanguage")
                                                   .set("language", entry.getKey())
                                                   .set("text", entry.getValue())
                                                   .set("field", getField().getName())
                                                   .handle();
                               });
        }
        super.onBeforeSaveChecks(entity);
    }

    /**
     * Bypasses {@link BaseMapProperty#getValueFromField(Object)} to access the actual MultiLanguageString entity.
     *
     * @see Property#getValueFromField(Object)
     */
    protected MultiLanguageString getMultiLanguageString(Object target) {
        try {
            return (MultiLanguageString) field.get(accessPath.apply(target));
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

    /**
     * Loads a value from a MongoDB datasource into a {@link MultiLanguageStringProperty} and skips language entries with null values.
     *
     * @param object the database value
     * @return the value which can be stored in the associated {@link MultiLanguageStringProperty} field
     */
    @Override
    @SuppressWarnings("unchecked")
    protected Object transformFromMongo(Value object) {
        Map<String, String> texts = new LinkedHashMap<>();
        for (Document document : (List<Document>) object.get()) {
            Object textValue = document.get(TEXT_PROPERTY);
            if (textValue != null) {
                texts.put(document.get(LANGUAGE_PROPERTY).toString(), textValue.toString());
            }
        }
        return texts;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object transformToMongo(Object object) {
        if (object instanceof List) {
            return object;
        }

        List<Document> texts = new ArrayList<>();
        ((Map<String, String>) object).forEach((language, text) -> {
            Document doc = new Document();
            doc.put(LANGUAGE_PROPERTY, language);
            doc.put(TEXT_PROPERTY, text);
            texts.add(doc);
        });
        return texts;
    }

    @Override
    protected Object transformFromElastic(Value object) {
        Map<String, String> result = new HashMap<>();
        Object value = object.get();
        if (value instanceof JSONObject) {
            ((JSONObject) value).forEach((language, text) -> {
                if (text instanceof String) {
                    result.put(language, (String) text);
                }
            });
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object transformToElastic(Object object) {
        if (object instanceof List) {
            return object;
        }

        JSONObject texts = new JSONObject();
        ((Map<String, String>) object).forEach(texts::fluentPut);
        return texts;
    }

    @Override
    public void describeProperty(JSONObject description) {
        description.put(IndexMappings.MAPPING_TYPE, "object");
        description.put(IndexMappings.MAPPING_DYNAMIC, true);
        transferOption(IndexMappings.MAPPING_ENABLED, getAnnotation(IndexMode.class), IndexMode::indexed, description);
    }

    @Override
    public boolean doesEnableDynamicMappings() {
        return true;
    }
}
