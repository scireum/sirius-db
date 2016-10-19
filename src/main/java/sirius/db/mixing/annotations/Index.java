/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.annotations;

import sirius.db.mixing.Schema;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines an additional index on an {@link sirius.db.mixing.Entity}.
 * <p>
 * The index will be picked up and created by {@link Schema#computeRequiredSchemaChanges()}
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(Indices.class)
public @interface Index {

    /**
     * Contains the name of the index.
     *
     * @return the name of the index
     */
    String name();

    /**
     * Contains the columns being indexed.
     *
     * @return the columns which make up the index
     */
    String[] columns();

    /**
     * Determines if the index implies an unique constraint on the combination of the given columns.
     *
     * @return <tt>true</tt> if an unique constraint is implied, <tt>false</tt> otherwise
     */
    boolean unique() default false;
}
