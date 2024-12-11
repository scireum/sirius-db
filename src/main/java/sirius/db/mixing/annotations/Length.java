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
 * Specifies the column length, most probably of string (CHAR) columns.
 * <p>
 * This annotation has no effect on properties other than a string, except the columns
 * definitions for Clickhouse, where the length is used also for other types, for example int,
 * to define the int size to use.
 *
 * @see sirius.db.jdbc.schema.ClickhouseDatabaseDialect
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Length {

    /**
     * The maximal length of the column.
     *
     * @return the maximal length of the column
     */
    int value();
}
