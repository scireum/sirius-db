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
import sirius.db.es.ElasticEntity;
import sirius.db.es.IndexMappings;
import sirius.db.es.annotations.ESOption;
import sirius.db.es.annotations.IndexMode;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;

/**
 * Represents a timestamp property which contains a date along with a time value. This is used to represents fields of
 * type {@link LocalDateTime}.
 */
public class ESLocalDateTimeProperty extends Property implements ESPropertyInfo {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return ElasticEntity.class.isAssignableFrom(descriptor.getType())
                   && LocalDateTime.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new ESLocalDateTimeProperty(descriptor, accessPath, field));
        }
    }

    protected ESLocalDateTimeProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    public Object transformValue(Value value) {
        return value.asLocalDateTime(null);
    }

    @Override
    protected Object transformFromDatasource(Value object) {
        String valueAsString = object.asString();
        if (Strings.isEmpty(valueAsString)) {
            return null;
        }

        if (valueAsString.contains("+")) {
            return LocalDateTime.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(valueAsString));
        } else {
            return LocalDateTime.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(valueAsString));
        }
    }

    @Override
    protected Object transformToDatasource(Object object) {
        if (!(object instanceof LocalDateTime)) {
            return null;
        }

        return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(((LocalDateTime) object).atZone(ZoneId.systemDefault()));
    }

    @Override
    public void describeProperty(JSONObject description) {
        description.put(IndexMappings.MAPPING_TYPE, "date");
        transferOption(IndexMappings.MAPPING_STORED, IndexMode::stored, ESOption.ES_DEFAULT, description);
        transferOption(IndexMappings.MAPPING_INDEXED, IndexMode::indexed, ESOption.ES_DEFAULT, description);
        transferOption(IndexMappings.MAPPING_DOC_VALUES, IndexMode::indexed, ESOption.ES_DEFAULT, description);
    }
}
