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
 * Marks a property as unique.
 * <p>
 * A value in this column must only occur once. If <tt>within</tt> is filled, the value must only occur once while
 * having the same values for the properties enumerated in <tt>within</tt>.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Unique {

    /**
     * Names properties which must also match fro two entities to trigger the unique check.
     *
     * @return the list of properties which determine the scope of the uniqueness
     */
    String[] within() default {};

    /**
     * Determines if the <tt>null</tt> also must be unique.
     *
     * @return <tt>true</tt> if <tt>null</tt> also must occur at most once, <tt>false</tt> (default) otherwise
     */
    boolean includingNull() default false;
}
