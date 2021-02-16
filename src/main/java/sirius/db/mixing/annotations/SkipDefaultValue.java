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
 * Instructs the entity mapper to not store a value in the database if the current value is a default value.
 * <p>
 * This can e.g. be used for MongoDB where we can simply skip fields which are <tt>null</tt> or <tt>false</tt>, as when
 * reading them back, the property will restore the default value anyway.
 * <p>
 * Note that this is not enabled by default as it might lead to unexpected behavior in queries. For example using
 * {@code db.collection.find({"non_existent_flag", false})} will not return document which <tt>false</tt> has been
 * skipped by not storing a value at all.
 * <p>
 * The bottom line is, that this can be used to "optimize away" sparse values <b>which are not used in queries</b>.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SkipDefaultValue {
}
