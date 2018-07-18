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
import java.sql.Date;
import java.sql.Types;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;

/**
 * Represents a date property which contains no associated time value. This is used to represents fields of type
 * {@link LocalDate}.
 */
public class LocalDateProperty extends Property implements ESPropertyInfo, SQLPropertyInfo {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return LocalDate.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new LocalDateProperty(descriptor, accessPath, field));
        }
    }

    protected LocalDateProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    public Object transformValue(Value value) {
        return NLS.parseUserString(LocalDate.class, value.asString());
    }

    @Override
    protected Object transformFromJDBC(Value data) {
        Object object = data.get();
        if (object == null) {
            return null;
        }
        return ((Date) object).toLocalDate();
    }

    @Override
    protected Object transformFromElastic(Value object) {
        String valueAsString = object.asString();
        if (Strings.isEmpty(valueAsString)) {
            return null;
        }

        if (valueAsString.contains("+")) {
            return LocalDate.from(DateTimeFormatter.ISO_OFFSET_DATE.parse(valueAsString));
        } else {
            return LocalDate.from(DateTimeFormatter.ISO_LOCAL_DATE.parse(valueAsString));
        }
    }

    @Override
    protected Object transformFromMongo(Value object) {
        return object.asLocalDate(null);
    }

    @Override
    protected Object transformToJDBC(Object object) {
        return object == null ? null : Date.valueOf((LocalDate) object);
    }

    @Override
    protected Object transformToElastic(Object object) {
        if (!(object instanceof LocalDate)) {
            return null;
        }

        return DateTimeFormatter.ISO_LOCAL_DATE.format((LocalDate) object);
    }

    @Override
    protected Object transformToMongo(Object object) {
        if (!(object instanceof LocalDate)) {
            return null;
        }

        return QueryBuilder.FILTERS.transform(object);
    }

    @Override
    public void contributeToTable(Table table) {
        table.getColumns().add(new TableColumn(this, Types.DATE));
    }

    @Override
    public void describeProperty(JSONObject description) {
        description.put(IndexMappings.MAPPING_TYPE, "date");
        transferOption(IndexMappings.MAPPING_STORED,
                       getAnnotation(IndexMode.class),
                       IndexMode::stored,
                       ESOption.ES_DEFAULT,
                       description);
        transferOption(IndexMappings.MAPPING_INDEXED,
                       getAnnotation(IndexMode.class),
                       IndexMode::indexed,
                       ESOption.ES_DEFAULT,
                       description);
        transferOption(IndexMappings.MAPPING_DOC_VALUES,
                       getAnnotation(IndexMode.class),
                       IndexMode::indexed,
                       ESOption.ES_DEFAULT,
                       description);
    }
}
