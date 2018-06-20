/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.jdbc.schema.SQLPropertyInfo;
import sirius.db.jdbc.schema.Table;
import sirius.db.jdbc.schema.TableColumn;
import sirius.kernel.commons.Amount;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.nls.NLS;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.function.Consumer;

/**
 * Represents an {@link Amount} field within a {@link Mixable}.
 */
public class AmountProperty extends Property implements SQLPropertyInfo {

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
                if (field.get(descriptor.getType().newInstance()) == null) {
                    Mixing.LOG.WARN("Field %s in %s is an Amount. Such fields should be initialized with Amount.NOTHING"
                                    + " as an amount should never be null!",
                                    field.getName(),
                                    field.getDeclaringClass().getName());
                }
            } catch (Exception e) {
                Mixing.LOG.WARN(
                        "An error occured while ensuring that the initial value of %s in %s is Amount.NOTHING: %s (%s)",
                        field.getName(),
                        field.getDeclaringClass().getName(),
                        e.getMessage(),
                        e.getClass().getName());
            }
            propertyConsumer.accept(amountProperty);
        }
    }

    AmountProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    public Object transformValue(Value value) {
        if (value.isFilled()) {
            return NLS.parseUserString(Amount.class, value.asString());
        }
        if (this.isNullable() || Strings.isEmpty(defaultValue)) {
            return Amount.NOTHING;
        }
        return Value.of(defaultValue).getAmount();
    }

    @Override
    protected Object transformToDatasource(Object object) {
        return object == null || ((Amount) object).isEmpty() ? null : ((Amount) object).getAmount();
    }

    @Override
    protected Object transformFromDatasource(Value data) {
        Object object = data.get();
        if (object == null) {
            return Amount.NOTHING;
        }
        if (object instanceof Double) {
            return Amount.of((Double) object);
        }
        return Amount.of((BigDecimal) object);
    }

    @Override
    public void contributeToTable(Table table) {
        TableColumn column = new TableColumn(this, Types.DECIMAL);
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

        table.getColumns().add(column);
    }

    @Override
    protected void checkNullability(Object propertyValue) {
        super.checkNullability(propertyValue);

        if (!isNullable() && ((Amount) propertyValue).isEmpty()) {
            throw Exceptions.createHandled().withNLSKey("Property.fieldNotNullable").set("field", getLabel()).handle();
        }
    }

}
