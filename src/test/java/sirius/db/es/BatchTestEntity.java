/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import sirius.db.mixing.Mapping;

public class BatchTestEntity extends VersionedEntity {

    public static final Mapping VALUE = Mapping.named("value");
    private int value;

    public int getValue() {
        return value;
    }

    public BatchTestEntity withValue(int value) {
        this.value = value;
        return this;
    }
}
