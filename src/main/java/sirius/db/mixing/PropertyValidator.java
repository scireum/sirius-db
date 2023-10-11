/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.kernel.di.std.AutoRegister;
import sirius.kernel.di.std.Named;

import java.util.function.Consumer;

/**
 * Permits to validate a property before it is written to the database.
 */
@AutoRegister
public interface PropertyValidator extends Named {

    /**
     * Validates the given value and reports any warnings/errors to the given consumer.
     *
     * @param value              the value to validate
     * @param validationConsumer can be used to report validation errors
     */
    void validate(Property property, Object value, Consumer<String> validationConsumer);

    /**
     * Validates the given value and reports any warnings/errors to the given consumer.
     * <p>
     * Throwing any exception will abort the write operation.
     *
     * @param value the value to validate
     */
    void beforeSave(Property property, Object value);
}
