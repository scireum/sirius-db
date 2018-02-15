/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import sirius.db.mixing.AccessPath;
import sirius.db.mixing.Entity;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.OMA;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.annotations.Lob;
import sirius.db.mixing.annotations.Trim;
import sirius.db.mixing.schema.TableColumn;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import java.lang.reflect.Field;
import java.sql.Clob;
import java.sql.Types;
import java.util.function.Consumer;

/**
 * Represents an {@link String} field within a {@link sirius.db.mixing.Mixable}.
 */
public class StringProperty extends Property {

    private final boolean trim;
    private final boolean lob;

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

    StringProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
        this.trim = field.isAnnotationPresent(Trim.class);
        this.lob = field.isAnnotationPresent(Lob.class);
    }

    @Override
    protected Object transformToColumn(Object object) {
        if (!lob && object != null && ((String) object).length() > length) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .withNLSKey("StringProperty.dataTruncation")
                            .set("value", Strings.limit(object, 30))
                            .set("field", getLabel())
                            .set("length", ((String) object).length())
                            .set("maxLength", length)
                            .handle();
        }
        return object;
    }

    @Override
    protected void setValueToField(Object value, Object target) {
        Object effectiveValue = value;
        if (effectiveValue instanceof Clob) {
            try {
                effectiveValue = ((Clob) value).getSubString(1, (int) ((Clob) value).length());
            } catch (Exception e) {
                throw Exceptions.handle()
                                .to(OMA.LOG)
                                .error(e)
                                .withSystemErrorMessage("Cannot read CLOB property %s of %s (%s): %s (%s)",
                                                        getColumnName(),
                                                        getDescriptor().getType().getName(),
                                                        getDescriptor().getTableName())
                                .handle();
            }
        }
        if (trim) {
            if (effectiveValue != null) {
                effectiveValue = ((String) effectiveValue).trim();
            }
            if ("".equals(effectiveValue)) {
                effectiveValue = null;
            }
        }
        super.setValueToField(effectiveValue, target);
    }

    @Override
    public Object transformValue(Value value) {
        if (value.isFilled()) {
            return value.asString();
        } else {
            if (this.isNullable() || Strings.isEmpty(defaultValue)) {
                return null;
            } else {
                return defaultValue;
            }
        }
    }

    @Override
    public void onBeforeSaveChecks(Entity entity) {
        if (trim) {
            String value = (String) getValue(entity);
            if (value != null) {
                value = value.trim();
                if (value.isEmpty()) {
                    value = null;
                }
                setValue(entity, value);
            }
        }
        super.onBeforeSaveChecks(entity);
    }

    @Override
    protected int getSQLType() {
        return Types.CHAR;
    }

    @Override
    protected TableColumn createColumn() {
        TableColumn column = super.createColumn();
        if (lob) {
            column.setType(Types.CLOB);
        } else {
            if (column.getLength() <= 0) {
                OMA.LOG.WARN("Error in property '%s' ('%s' of '%s'): A string property needs a length!"
                             + " (Use @Length to specify one). Defaulting to 255.",
                             getName(),
                             field.getName(),
                             field.getDeclaringClass().getName());
                column.setLength(255);
            }
        }
        return column;
    }
}
