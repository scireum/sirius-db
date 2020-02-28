/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.annotations;

import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.Mixable;
import sirius.kernel.di.std.Priorized;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to mark methods in {@link Mixable}s which will be called before an entity is saved.
 * <p>
 * If you need different logic for updated and newly created entities, use {@link BaseEntity#isNew()} in your method.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface BeforeSave {

    /**
     * Determines the execution order of all before save handlers.
     *
     * @return the execution priority. Handlers will be executed in an ascending order, the one with the lowest value
     * will be executed first.
     */
    int priority() default Priorized.DEFAULT_PRIORITY;
}
