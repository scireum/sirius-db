/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.alibaba.fastjson2.JSONObject;
import sirius.db.mixing.EntityDescriptor;
import sirius.kernel.di.std.AutoRegister;

/**
 * Customizes the settings block of an {@link ElasticEntity} when creating a new index in Elasticsearch.
 * <p>
 * This class needs to be {@link sirius.kernel.di.std.Register registered} and can then be referenced by placing
 * a {@link sirius.db.es.annotations.CustomSettings} annotation on the appropriate entity classes.
 */
@AutoRegister
public interface SettingsCustomizer {

    /**
     * Customizes the given settings object for the given entity (descriptor).
     *
     * @param descriptor     the descriptor of the entity to customize
     * @param settingsObject the <tt>settings</tt> object which will be passed to Elasticsearch when creating a new
     *                       index.
     */
    void customizeSettings(EntityDescriptor descriptor, JSONObject settingsObject);
}
