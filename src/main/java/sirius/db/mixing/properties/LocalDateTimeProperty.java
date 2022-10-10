/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import com.alibaba.fastjson2.JSONObject;
import sirius.db.es.ESPropertyInfo;
import sirius.db.es.IndexMappings;
import sirius.db.es.annotations.IndexMode;
import sirius.db.jdbc.Databases;
import sirius.db.jdbc.schema.SQLPropertyInfo;
import sirius.db.jdbc.schema.Table;
import sirius.db.jdbc.schema.TableColumn;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.annotations.DefaultValue;
import sirius.db.mongo.QueryBuilder;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.nls.NLS;

import java.lang.reflect.Field;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.function.Consumer;

/**
 * Represents a timestamp property which contains a date along with a time value. This is used to represent fields of
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
        if (value.is(LocalDateTime.class)) {
            return ((LocalDateTime) (value.get())).truncatedTo(ChronoUnit.MILLIS);
        }
        if (value.is(LocalDate.class)) {
            return value.get(LocalDate.class, null).atStartOfDay();
        }
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

        try {
            return LocalDateTime.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(valueAsString))
                                .truncatedTo(ChronoUnit.MILLIS);
        } catch (DateTimeParseException e) {
            return LocalDateTime.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(valueAsString))
                                .truncatedTo(ChronoUnit.MILLIS);
        }
    }

    @Override
    protected Object transformFromMongo(Value object) {
        LocalDateTime localDateTime = object.asLocalDateTime(null);
        return localDateTime == null ? null : localDateTime.truncatedTo(ChronoUnit.MILLIS);
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
        transferOption(IndexMappings.MAPPING_STORED, getAnnotation(IndexMode.class), IndexMode::stored, description);
        transferOption(IndexMappings.MAPPING_INDEX, getAnnotation(IndexMode.class), IndexMode::indexed, description);
        transferOption(IndexMappings.MAPPING_DOC_VALUES,
                       getAnnotation(IndexMode.class),
                       IndexMode::docValues,
                       description);
    }

    @Override
    public void contributeToTable(Table table) {
        table.getColumns().add(new TableColumn(this, Types.BIGINT));
    }

    /**
     * Overrides the default behavior, as the initial value of a temporal property is not suited for a default.
     * <p>
     * The initial value will commonly be a temporal value and thus not a constant.
     * Therefore, we ignore the initial value here, and only check for a {@link DefaultValue} annotation on the field.
     */
    @Override
    protected void determineDefaultValue() {
        DefaultValue defaultValueAnnotation = field.getAnnotation(DefaultValue.class);
        if (defaultValueAnnotation != null) {
            this.defaultValue = Value.of(transformValueFromImport(Value.of(defaultValueAnnotation.value())));
        }
    }
}
