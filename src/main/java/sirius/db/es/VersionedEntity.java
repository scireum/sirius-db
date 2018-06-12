/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.annotations.Transient;

/**
 * Represents the base class for all entities in Elasticsearch which support <tt>optimistic locking</tt>.
 *
 * @see Elastic#tryUpdate(BaseEntity)
 * @see Elastic#tryDelete(BaseEntity)
 */
public abstract class VersionedEntity extends ElasticEntity {

    /**
     * Contains the entity version.
     * <p>
     * This isn't a mapped field, as in Elasticsearch the version is sorted in <tt>_version</tt> which is a special field.
     */
    @Transient
    protected int version = 0;

    public int getVersion() {
        return version;
    }

    /**
     * Note that only the framework must use this to specify the version of the entity.
     *
     * @param version the version of this entity
     */
    protected void setVersion(int version) {
        this.version = version;
    }
}
