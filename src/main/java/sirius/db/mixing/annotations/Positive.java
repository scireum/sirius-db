/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that a {@link sirius.db.mixing.properties.NumberProperty} must have a positive value.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Positive {
    /**
     * Determines if <tt>0</tt> is a valid value for this property.
     *
     * @return <tt>true</tt> if <tt>0</tt> is a valid value for this property, <tt>false</tt> (default) otherwise
     */
    boolean includeZero() default false;
}
