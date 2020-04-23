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
 * Can be placed on a {@link sirius.db.mixing.BaseEntity entity} to use an alternative class when fetching i18n keys.
 * <p>
 * By default {@link sirius.db.mixing.EntityDescriptor} and {@link sirius.db.mixing.Property} use the class which
 * contains a field or the entity class itself when building property keys. Using this annotation, another class can
 * be used to provide the i18n keys.
 * <p>
 * This is mostly used by database independent frameworks. These define a common interface with shared
 * {@link sirius.db.mixing.Mapping mappings} which is then implemented by subclasses of the respective entity types.
 * To also share the i18n keys, the database specific entities can delegate all i18n keys by using the interface
 * as translation source.
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface TranslationSource {

    /**
     * Contains the class which is then used to compute the i18n keys of the annotated entity class.
     *
     * @return the class which is used to provide common names for i18n keys
     */
    Class<?> value();
}
