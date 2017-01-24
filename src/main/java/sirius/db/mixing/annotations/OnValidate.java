/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to mark methods in {@link sirius.db.mixing.Mixable}s which will be called once is validated.
 * <p>
 * Note that such a method must accept a <tt>Consumer&lt;String&gt;</tt> as first parameter or the entity
 * itself and the consumer as second parameter, when called within a mixin.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface OnValidate {
}
