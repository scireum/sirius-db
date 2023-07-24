/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.annotations;

import sirius.db.mixing.Mixable;
import sirius.kernel.di.std.Priorized;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to mark methods in {@link Mixable}s which will be called before an entity is deleted.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface BeforeDelete {

    /**
     * Determines the execution order of all before delete handlers.
     *
     * @return the execution priority. Handlers will be executed in ascending order, the one with the lowest value
     * will be executed first.
     */
    int priority() default Priorized.DEFAULT_PRIORITY;
}
