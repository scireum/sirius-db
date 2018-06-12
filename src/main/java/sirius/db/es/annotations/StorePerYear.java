/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an entity which is stored in one index per year.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface StorePerYear {

    /**
     * Contains the name of the property which contains a date or date/time field used to determine the "year" for a
     * given entity.
     *
     * @return the name of the property used to determine the "year" of a given entity
     */
    String value();
}
