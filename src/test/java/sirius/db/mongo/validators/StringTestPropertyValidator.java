/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.validators;

import sirius.db.mixing.Property;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import javax.annotation.Nonnull;
import java.util.function.Consumer;

@Register
public class StringTestPropertyValidator implements sirius.db.mixing.PropertyValidator {
    @Override
    public void validate(Property property, Object value, Consumer<String> validationConsumer) {
        if (isInvalidTestString(value)) {
            validationConsumer.accept("Invalid value!");
        }
    }

    @Override
    public void beforeSave(Property property, Object value) {
        if (isInvalidTestString(value)) {
            throw Exceptions.createHandled().withSystemErrorMessage("Invalid value!").handle();
        }
    }

    private static boolean isInvalidTestString(Object value) {
        return value instanceof String text && "invalid".equals(text);
    }

    @Nonnull
    @Override
    public String getName() {
        return "string-test";
    }
}
