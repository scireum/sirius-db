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
import sirius.db.mixing.Property;
import sirius.db.mixing.annotations.MaxValue;
import sirius.db.mixing.annotations.MinValue;
import sirius.db.mixing.annotations.Positive;
import sirius.kernel.commons.Amount;
import sirius.kernel.commons.Value;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.util.Objects;

/**
 * Base class for number fields within a {@link Mixable}.
 * <p>
 * Parses the annotations of the field to determine its range and contains a method to assert a parsed value is within that range.
 */
public abstract class NumberProperty extends Property {

    /**
     * Minimal value (including) of this property, calculated from the Annotations present on the field.
     */
    private Amount minValue = Amount.NOTHING;
    /**
     * Maximal value (including) of this property, calculated from the Annotations present on the field.
     */
    private Amount maxValue = Amount.NOTHING;

    /**
     * Creates a new property for the given descriptor, access path and field.
     *
     * @param descriptor the descriptor which owns the property
     * @param accessPath the access path required to obtain the target object which contains the field
     * @param field      the field which stores the database value
     */
    protected NumberProperty(@Nonnull EntityDescriptor descriptor,
                             @Nonnull AccessPath accessPath,
                             @Nonnull Field field) {
        super(descriptor, accessPath, field);
        setupRange(field);
    }

    private void setupRange(@Nonnull Field field) {
        if (field.isAnnotationPresent(Positive.class)) {
            minValue = field.getAnnotation(Positive.class).includeZero() ? Amount.ZERO : Amount.ONE;
        }
        MinValue minValueAnnotation = field.getAnnotation(MinValue.class);
        if (minValueAnnotation != null && Amount.of(minValueAnnotation.value()).isGreaterThan(minValue)) {
            minValue = Amount.of(minValueAnnotation.value());
        }
        MaxValue maxValueAnnotation = field.getAnnotation(MaxValue.class);
        if (maxValueAnnotation != null) {
            maxValue = Amount.of(maxValueAnnotation.value());
        }
    }

    /**
     * Ensure the given value is within the range described by this fields annotations.
     * 
     * @param amount the value of the of the field, wrapped in an {@link Amount}
     */
    protected void assertValueIsInRange(Amount amount) {
        if (minValue.isFilled() && minValue.isGreaterThan(amount) && !(amount.isEmpty() && isNullable())) {
            throw illegalFieldValue(Value.of(amount));
        }
        if (maxValue.isFilled() && maxValue.isLessThan(amount)) {
            throw illegalFieldValue(Value.of(amount));
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof NumberProperty)) {
            return false;
        }
        if (!((NumberProperty) obj).maxValue.equals(maxValue) || !((NumberProperty) obj).minValue.equals(minValue)) {
            return false;
        }
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxValue, minValue, descriptor, name);
    }
}
