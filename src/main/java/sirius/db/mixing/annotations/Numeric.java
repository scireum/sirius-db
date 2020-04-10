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
 * Specifies the numeric precision of a NUMBER column.
 * <p>
 * If a column must sore values with 12 digits and 3 decimal places, the precision would by <tt>15</tt> and scale
 * <tt>3</tt>.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Numeric {

    /**
     * The total number of digits which can be stored in that column without rounding.
     *
     * @return the total number of digits
     */
    int precision();

    /**
     * The number of decimal digits after the decimal separator.
     *
     * @return the number of digits after the decimal separator
     */
    int scale();
}
