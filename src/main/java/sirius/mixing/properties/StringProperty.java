/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.properties;

import sirius.kernel.di.std.Register;
import sirius.mixing.*;
import sirius.mixing.schema.TableColumn;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.function.Consumer;

/**
 * Created by aha on 15.04.15.
 */
public class StringProperty extends Property {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(Field field) {
            return String.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            propertyConsumer.accept(new StringProperty(descriptor, accessPath, field));
        }

    }

    public StringProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    public void onBeforeSave(Entity entity) {
        String value = (String) getValue(entity);
        if (value != null) {
            value = value.trim();
            if ("".equals(value)) {
                value = null;
            }
            setValue(entity, value);
        }
        super.onBeforeSave(entity);
    }

    @Override
    protected int getSQLType() {
        return Types.CHAR;
    }

    @Override
    protected void finalizeColumn(TableColumn column) {
        if (column.getLength() <= 0) {
            OMA.LOG.WARN(
                    "Error in property '%s' ('%s' of '%s'): A string property needs a length! (Use @Length to specify one). Defaulting to 255.",
                    getName(),
                    field.getName(),
                    field.getDeclaringClass().getName());
            column.setLength(255);
        }
    }
}
