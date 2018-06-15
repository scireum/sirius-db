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
 * Specifies the relation name to used for an entity.
 * <p>
 * By default the simple class name (all lowercase) is used as relation name. This annotation can be used to
 * provide a custom one.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface RelationName {

    /**
     * The relation name (table name / collection name / index name) to use.
     *
     * @return the name to use to represent an entity in the database
     */
    String value();
}
