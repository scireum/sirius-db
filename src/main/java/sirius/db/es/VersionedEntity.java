/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import sirius.db.mixing.annotations.Transient;

public abstract class VersionedEntity extends ElasticEntity {

    @Transient
    protected int version = 0;

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
