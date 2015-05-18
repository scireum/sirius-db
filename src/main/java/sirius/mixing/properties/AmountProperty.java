/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.properties;

import sirius.kernel.commons.Amount;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.nls.NLS;
import sirius.mixing.*;
import sirius.mixing.schema.TableColumn;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.function.Consumer;

/**
 * Created by aha on 15.04.15.
 */
public class AmountProperty extends Property {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(Field field) {
            return Amount.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new AmountProperty(descriptor, accessPath, field));
        }

    }


    public AmountProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    protected Object transformValue(Value value) {
        if (!value.isFilled()) {
            return isNullable() ? null : Amount.NOTHING;
        } else {
            return NLS.parseUserString(Amount.class, value.asString());
        }
    }

    @Override
    protected Object transformToColumn(Object object) {
        return object == null || ((Amount) object).isEmpty() ? null : ((Amount) object).getAmount();
    }

    @Override
    protected Object transformFromColumn(Object object) {
        if (object == null) {
            return isNullable() ? null : Amount.NOTHING;
        }
        if (object instanceof Double) {
            return Amount.of((Double) object);
        }
        return Amount.of((BigDecimal) object);
    }

    @Override
    protected int getSQLType() {
        return Types.DECIMAL;
    }

    @Override
    protected TableColumn createColumn() {
        TableColumn column = super.createColumn();
        if (column.getLength() > 0) {
            OMA.LOG.WARN("Error in property '%s' ('%s' of '%s'): An 'Amount' property must not specify a length!",
                         getName(),
                         field.getName(),
                         field.getDeclaringClass().getName());
        }
        if (column.getPrecision() <= 0) {
            OMA.LOG.WARN(
                    "Error in property '%s' ('%s' of '%s'): An 'Amount' property needs a precision. Defaulting to 15.",
                    getName(),
                    field.getName(),
                    field.getDeclaringClass().getName());
            column.setPrecision(15);
        }
        if (column.getScale() > column.getPrecision()) {
            OMA.LOG.WARN(
                    "Error in property '%s' ('%s' of '%s'): An 'Amount' must not have a higher scale than precision",
                    getName(),
                    field.getName(),
                    field.getDeclaringClass().getName());
            column.setScale(column.getPrecision());
        }
        return column;
    }
}
