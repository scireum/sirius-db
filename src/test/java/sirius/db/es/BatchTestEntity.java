/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.Versioned;

@Versioned
public class BatchTestEntity extends ElasticEntity {

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
