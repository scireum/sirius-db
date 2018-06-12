/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.OptimisticLockException;

/**
 * Enables optimistic locking for the annotated entity.
 *
 * @see OptimisticLockException
 * @see Mango#tryUpdate(BaseEntity)
 * @see Mango#tryDelete(BaseEntity)
 */
public abstract class VersionedEntity extends MongoEntity {

    /**
     * Stores the entity version in the database.
     * <p>
     * This must not be modified outside of the framework.
     */
    public static final Mapping VERSION = Mapping.named("version");
    protected int version = 0;

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
