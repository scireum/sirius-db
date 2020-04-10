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
 * Determines if an{@link sirius.db.mixing.BaseEntity entity} is complex to delete.
 * <p>
 * This can either be placed on the entity class or on any {@link BeforeDelete before delete handler} or
 * {@link AfterDelete after delete handler}.
 * <p>
 * If the value is set to <tt>true</tt> (default), the entity is considered complex to delete. If <tt>false</tt>
 * is specified, it isn't considered complex to delete, even if there are cascadeing actions (setting fields in other
 * entities to null or deleting other entities).
 */
@Documented
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ComplexDelete {

    /**
     * Specifies if the entity is complex to delete.
     *
     * @return <tt>true</tt> to mark it as complex, <tt>false</tt> to mark it as non-complex even if there are cascade
     * delete actions. Note that this can only be specified on the entity class but not for cascade delete handlers
     */
    boolean value() default true;
}
