/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.annotations;

import sirius.db.es.SettingsCustomizer;
import sirius.kernel.di.std.Register;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies a {@link SettingsCustomizer} to use for a given {@link sirius.db.es.ElasticEntity}.
 * <p>
 * This annotation needs to be placed on an entity class and will then invoke the given customizer during index
 * creation. This can be used to add additional settings when creating an index within Elasticsearh.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface CustomSettings {

    /**
     * Specifies the {@link SettingsCustomizer} to use.
     *
     * @return the type of customizer to use
     */
    Class<? extends SettingsCustomizer> value();
}
