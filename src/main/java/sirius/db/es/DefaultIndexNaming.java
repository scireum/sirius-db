/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import sirius.db.mixing.EntityDescriptor;
import sirius.kernel.di.std.Register;

/**
 * Supplies default naming conventions for indices and mappings.
 */
@Register
public class DefaultIndexNaming implements IndexNaming {

    @Override
    public int getPriority() {
        return 150;
    }

    /**
     * Use the {@link sirius.db.mixing.annotations.RelationName relation name} or if none is given the lower case
     * {@link Class#getSimpleName() name of the entity} as the index name.
     * <p>
     * Can be overridden by setting {@code mixing.legacy.<Entity>.tableName}.
     *
     * @param descriptor the descriptor of the entity to return the index name for
     * @return the relation name
     */
    @Override
    public String determineIndexName(EntityDescriptor descriptor) {
        return descriptor.getRelationName();
    }

    /**
     * Use the lower case {@link Class#getSimpleName() name of the entity} as the mapping name.
     *
     * @param descriptor the descriptor of the entity to return the mapping name for
     * @return the lower case entity name
     */
    @Override
    public String determineMappingName(EntityDescriptor descriptor) {
        return descriptor.getType().getSimpleName().toLowerCase();
    }
}
