/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import sirius.db.mixing.EntityDescriptor;
import sirius.kernel.di.std.Priorized;

/**
 * Can be supplied to enforce naming conventions for indices and mappings in ElasticSearch on a per product basis.
 * <p>
 * The priority needs to be lower than {@link #DEFAULT_PRIORITY}.
 */
public interface IndexNaming extends Priorized {

    @Override
    default int getPriority() {
        return DEFAULT_PRIORITY;
    }

    /**
     * Determines the name of the index for the given entity.
     *
     * @param descriptor the descriptor of the entity to return the index name for
     * @return the index name
     */
    String determineIndexName(EntityDescriptor descriptor);

    /**
     * Determines the name of the mapping for the given entity.
     *
     * @param descriptor the descriptor of the entity to return the mapping name for
     * @return the mapping name
     */
    String determineMappingName(EntityDescriptor descriptor);
}
