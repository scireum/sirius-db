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
 * Provides a container to make {@link Index} repeatable.
 * <p>
 * As of Java 8 several <tt>Index</tt> annotation can be placed on an entity class, therefore this wrapper is only used
 * by the compiler and runtime to carry those annotations.
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Indices {

    /**
     * Contains all index annoations placed on an entity class.
     *
     * @return an array of all index annotations
     */
    Index[] value();
}
