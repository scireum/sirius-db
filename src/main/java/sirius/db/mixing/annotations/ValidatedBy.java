/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.annotations;

import sirius.db.mixing.PropertyValidator;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Allows to specify a custom {@link PropertyValidator} for a property.
 * <p>
 * This validator will be invoked before the property is written to the database.
 * It can be used to perform custom validation that is the same for fields across multiple entities.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ValidatedBy {

    /**
     * Specifies the validator to use.
     */
    Class<? extends PropertyValidator> value();
}
