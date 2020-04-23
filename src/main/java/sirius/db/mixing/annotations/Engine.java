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
 * Can be placed on a {@link sirius.db.jdbc.SQLEntity} to specify the database engine to use.
 * <p>
 * Use e.g. <tt>InnoDB</tt> for MySQL or <tt>MergeTree(EventDate, (CounterID, EventDate), 8192)</tt>
 * for Clickhouse.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Engine {

    /**
     * Contains the engine specification to apply on the generated table.
     *
     * @return the engine used for the annotated type
     */
    String value();
}
