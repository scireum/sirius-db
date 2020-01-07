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
import sirius.db.es.annotations.IndexMode;
import sirius.db.jdbc.Capability;
import sirius.db.jdbc.OMA;
import sirius.db.jdbc.schema.SQLPropertyInfo;
import sirius.db.jdbc.schema.Table;
import sirius.db.jdbc.schema.TableColumn;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.annotations.Lob;
import sirius.db.mixing.types.StringList;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.commons.Values;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Represents a {@link StringList} field within a {@link Mixable}.
 * <p>
 * This property works for elastic search, mongo and sql.
 * <p>
 * If the SQL database does not support lists, the list will be stored as text as comma seperated values. The length
 * limit defined via the {@link sirius.db.mixing.annotations.Length Length annotation} will be applied to the text
 * field.
 */
public class StringListProperty extends Property implements ESPropertyInfo, SQLPropertyInfo {

    @Part
    private static OMA oma;

    private static final String[] EMPTY_STRING_ARRAY = {};

    private Boolean dbHasCapabilityLists;

    private final boolean lob;

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return StringList.class.equals(field.getType());
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

            propertyConsumer.accept(new StringListProperty(descriptor, accessPath, field));
        }
    }

    protected StringListProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
        this.lob = field.isAnnotationPresent(Lob.class);
    }

    @Override
    protected Object getValueFromField(Object target) {
        return ((StringList) super.getValueFromField(target)).data();
    }

    @Override
    public Object getValueAsCopy(Object entity) {
        Object target = accessPath.apply(entity);
        return ((StringList) super.getValueFromField(target)).copyList();
    }

    @Override
    public Object transformValue(Value value) {
        if (value.isEmptyString()) {
            return null;
        }

        return value.get();
    }

    @Override
    protected Object transformToElastic(Object object) {
        return object;
    }

    @Override
    protected Object transformToMongo(Object object) {
        return object;
    }

    @Override
    protected Object transformFromElastic(Value object) {
        return object.get();
    }

    @Override
    protected Object transformFromMongo(Value object) {
        return object.get();
    }

    @Override
    protected Object transformFromJDBC(Value object) {
        if (hasDBCapabilityLists()) {
            if (object.isFilled()) {
                return unpackListOrArray(object);
            } else {
                return EMPTY_STRING_ARRAY;
            }
        }
        return Arrays.stream(object.asString().split(",")).filter(Strings::isFilled).collect(Collectors.toList());
    }

    private Object unpackListOrArray(Value object) {
        if (object.get() instanceof Array) {
            try {
                return Arrays.asList((String[]) ((Array) object.get()).getArray());
            } catch (SQLException e) {
                throw Exceptions.handle(Mixing.LOG, e);
            }
        } else {
            return Arrays.asList(object.coerce(String[].class, EMPTY_STRING_ARRAY));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object transformToJDBC(Object object) {
        if (hasDBCapabilityLists() && object instanceof Collection) {
            return ((Collection<String>) object).toArray();
        }
        String data = Strings.join((Collection<?>) object, ",");
        if (length > 0 && data.length() > length) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .withNLSKey("StringProperty.dataTruncation")
                            .set("value", Strings.limit(data, 30))
                            .set("field", getFullLabel())
                            .set("length", data.length())
                            .set("maxLength", length)
                            .handle();
        }
        return data;
    }

    private boolean hasDBCapabilityLists() {
        if (dbHasCapabilityLists == null) {
            dbHasCapabilityLists = oma.getDatabase(descriptor.getRealm()).hasCapability(Capability.LISTS);
        }
        return dbHasCapabilityLists;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void setValueToField(Object value, Object target) {
        ((StringList) super.getValueFromField(target)).setData((List<String>) value);
    }

    @Override
    public void describeProperty(JSONObject description) {
        description.put("type", "keyword");
        transferOption(IndexMappings.MAPPING_STORED, getAnnotation(IndexMode.class), IndexMode::stored, description);
        transferOption(IndexMappings.MAPPING_INDEX, getAnnotation(IndexMode.class), IndexMode::indexed, description);
        transferOption(IndexMappings.MAPPING_DOC_VALUES,
                       getAnnotation(IndexMode.class),
                       IndexMode::docValues,
                       description);
    }

    @Override
    public void parseValues(Object e, Values values) {
        if (values.length() == 1) {
            setValue(e,
                     Arrays.stream(values.at(0).asString().split(","))
                           .map(Strings::trim)
                           .filter(Objects::nonNull)
                           .collect(Collectors.toList()));
            return;
        }

        List<String> stringData = new ArrayList<>();
        for (int i = 0; i < values.length(); i++) {
            values.at(i).ifFilled(value -> stringData.add(value.toString()));
        }
        setValue(e, stringData);
    }

    @Override
    public void contributeToTable(Table table) {
        if (hasDBCapabilityLists()) {
            table.getColumns().add(new TableColumn(this, Types.ARRAY));
        } else if (lob) {
            table.getColumns().add(new TableColumn(this, Types.CLOB));
        } else {
            table.getColumns().add(new TableColumn(this, Types.CHAR));
        }
    }
}
