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
import sirius.db.jdbc.Database;
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
import sirius.db.mixing.annotations.Numeric;
import sirius.kernel.commons.Amount;
import sirius.kernel.commons.NumberFormat;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.nls.NLS;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Types;
import java.util.function.Consumer;

/**
 * Represents an {@link Amount} field within a {@link Mixable}.
 */
public class AmountProperty extends NumberProperty implements SQLPropertyInfo, ESPropertyInfo {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return Amount.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            AmountProperty amountProperty = new AmountProperty(descriptor, accessPath, field);

            try {
                if (field.get(accessPath.apply(descriptor.getType().getDeclaredConstructor().newInstance())) == null) {
                    Mixing.LOG.WARN("Field %s in %s is an Amount. Such fields should be initialized with Amount.NOTHING"
                                    + " as an amount should never be null!",
                                    field.getName(),
                                    field.getDeclaringClass().getName());
                }
            } catch (Exception e) {
                Mixing.LOG.WARN(
                        "An error occurred while ensuring that the initial value of %s in %s is Amount.NOTHING: %s (%s)",
                        field.getName(),
                        field.getDeclaringClass().getName(),
                        e.getMessage(),
                        e.getClass().getName());
            }
            propertyConsumer.accept(amountProperty);
        }
    }

    @Part
    private static OMA oma;

    AmountProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    public Object transformValue(Value value) {
        if (value.isFilled()) {
            return NLS.parseUserString(Amount.class, value.asString());
        }
        if (this.isNullable() || defaultValue.isEmptyString()) {
            return Amount.NOTHING;
        }
        return defaultValue.getAmount();
    }

    @Override
    protected Object transformValueFromImport(Value value) {
        if (value.is(Amount.class)) {
            return value.get();
        }

        if (value.isFilled()) {
            return Amount.ofMachineString(value.asString());
        }

        return transformValue(value);
    }

    @Override
    protected BigDecimal transformToJDBC(Object object) {
        return object == null || ((Amount) object).isEmpty() ? null : ((Amount) object).getAmount();
    }

    @Override
    public String getColumnDefaultValue() {
        if (defaultValue.isNull()) {
            return null;
        }
        Object defaultData = transformToDatasource(OMA.class, defaultValue.get());
        if (defaultData == null) {
            return null;
        }
        // the resulting string needs to match the string representation in the DB exactly,
        // else a schema change will be issued.
        NumberFormat format = getAnnotation(Numeric.class).map(numeric -> {
            return new NumberFormat(numeric.scale(), RoundingMode.HALF_UP, NLS.getMachineFormatSymbols(), false, null);
        }).orElse(NumberFormat.MACHINE_THREE_DECIMAL_PLACES);
        return Amount.of((BigDecimal) defaultData).toString(format).asString();
    }

    @Override
    protected Object transformToElastic(Object object) {
        return object == null || ((Amount) object).isEmpty() ? null : ((Amount) object).getAmount().toPlainString();
    }

    @Override
    protected Object transformToMongo(Object object) {
        return object == null || ((Amount) object).isEmpty() ? null : ((Amount) object).getAmount();
    }

    @Override
    protected Object transformFromJDBC(Value data) {
        Object object = data.get();
        if (object == null) {
            return Amount.NOTHING;
        }
        if (object instanceof Double) {
            return Amount.of((Double) object);
        }
        if (object instanceof Integer) {
            return Amount.of((Integer) object);
        }
        return Amount.of((BigDecimal) object);
    }

    @Override
    protected Object transformFromElastic(Value data) {
        String valueAsString = data.asString();
        if (Strings.isEmpty(valueAsString)) {
            return Amount.NOTHING;
        }
        return Amount.of(new BigDecimal(valueAsString));
    }

    @Override
    protected Object transformFromMongo(Value object) {
        return object.getAmount();
    }

    @Override
    public void contributeToTable(Table table) {
        int sqlType = determineJDBCDatatype();
        TableColumn column = new TableColumn(this, sqlType);
        if (sqlType == Types.DECIMAL) {
            if (column.getLength() > 0) {
                Mixing.LOG.WARN("Error in property '%s' ('%s' of '%s'): An 'Amount' property must not specify a length!",
                                getName(),
                                field.getName(),
                                field.getDeclaringClass().getName());
            }
            if (column.getPrecision() <= 0) {
                Mixing.LOG.WARN("Error in property '%s' ('%s' of '%s'): An 'Amount' property needs a precision!"
                                + " Use @Numeric to specify one. Defaulting to 15.",
                                getName(),
                                field.getName(),
                                field.getDeclaringClass().getName());
                column.setPrecision(15);
            }
            if (column.getScale() > column.getPrecision()) {
                Mixing.LOG.WARN(
                        "Error in property '%s' ('%s' of '%s'): An 'Amount' must not have a higher scale than precision",
                        getName(),
                        field.getName(),
                        field.getDeclaringClass().getName());
                column.setScale(column.getPrecision());
            }
        }

        table.getColumns().add(column);
    }

    private int determineJDBCDatatype() {
        Database database = oma.getDatabase(descriptor.getRealm());
        if (database.hasCapability(Capability.DECIMAL_TYPE)) {
            return Types.DECIMAL;
        }

        return Types.DOUBLE;
    }

    @Override
    protected boolean isConsideredNull(Object propertyValue) {
        return propertyValue == null || ((Amount) propertyValue).isEmpty();
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
}
