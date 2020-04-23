/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.annotations;

import sirius.db.jdbc.SQLEntity;
import sirius.db.mixing.Composite;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as mixin for another target class (which is either an {@link SQLEntity} or a {@link Composite}).
 * <p>
 * A mixin can add properties to an entity or composite, which are not defined in the original class. This can be used
 * to defined customer extensions without modifying the standard classes.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Mixin {
    /**
     * The target class which will inherit all properties defined by this mixing.
     *
     * @return the target class to extend
     */
    Class<?> value();
}
