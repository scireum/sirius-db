/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import com.alibaba.fastjson.JSONObject;
import sirius.db.es.ESPropertyInfo;
import sirius.db.es.IndexMappings;
import sirius.db.es.annotations.ESOption;
import sirius.db.es.annotations.IndexMode;
import sirius.db.jdbc.Databases;
import sirius.db.jdbc.schema.SQLPropertyInfo;
import sirius.db.jdbc.schema.Table;
import sirius.db.jdbc.schema.TableColumn;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mongo.QueryBuilder;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.nls.NLS;

import java.lang.reflect.Field;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;

/**
 * Represents a timestamp property which contains a date along with a time value. This is used to represents fields of
 * type {@link LocalDateTime}.
 */
public class LocalDateTimeProperty extends Property implements ESPropertyInfo, SQLPropertyInfo {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return LocalDateTime.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new LocalDateTimeProperty(descriptor, accessPath, field));
        }
    }

    protected LocalDateTimeProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    public Object transformValue(Value value) {
        return NLS.parseUserString(LocalDateTime.class, value.asString());
    }

    @Override
    protected Object transformFromJDBC(Value object) {
        Object data = object.get();
        if (data == null) {
            return null;
        }
        return Databases.decodeLocalDateTime((long) data);
    }

    @Override
    protected Object transformFromElastic(Value object) {
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
    protected Object transformFromMongo(Value object) {
        return object.asLocalDateTime(null);
    }

    @Override
    protected Object transformToJDBC(Object object) {
        if (object == null) {
            return null;
        }
        return Databases.encodeLocalDateTime((LocalDateTime) object);
    }

    @Override
    protected Object transformToElastic(Object object) {
        if (!(object instanceof LocalDateTime)) {
            return null;
        }

        return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(((LocalDateTime) object).atZone(ZoneId.systemDefault()));
    }

    @Override
    protected Object transformToMongo(Object object) {
        if (!(object instanceof LocalDateTime)) {
            return null;
        }

        return QueryBuilder.FILTERS.transform(object);
    }

    @Override
    public void describeProperty(JSONObject description) {
        description.put(IndexMappings.MAPPING_TYPE, "date");
        transferOption(IndexMappings.MAPPING_STORED, IndexMode::stored, ESOption.ES_DEFAULT, description);
        transferOption(IndexMappings.MAPPING_INDEXED, IndexMode::indexed, ESOption.ES_DEFAULT, description);
        transferOption(IndexMappings.MAPPING_DOC_VALUES, IndexMode::indexed, ESOption.ES_DEFAULT, description);
    }

    @Override
    public void contributeToTable(Table table) {
        table.getColumns().add(new TableColumn(this, Types.BIGINT));
    }
}
