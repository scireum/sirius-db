/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.annotations;

import sirius.db.mixing.Entity;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Enables optimistic locking for the annotated entity.
 *
 * @see sirius.db.mixing.OptimisticLockException
 * @see sirius.db.mixing.OMA#tryUpdate(Entity)
 * @see sirius.db.mixing.OMA#tryDelete(Entity)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Versioned {
}
