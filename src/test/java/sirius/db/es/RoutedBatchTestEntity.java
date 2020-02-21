/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import sirius.db.es.annotations.RoutedBy;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.Versioned;

@Versioned
public class RoutedBatchTestEntity extends ElasticEntity {

    public static final Mapping VALUE = Mapping.named("value");
    private int value;

    public static final Mapping VALUE1 = Mapping.named("value1");
    @RoutedBy
    private int value1;

    public int getValue() {
        return value;
    }

    public RoutedBatchTestEntity withValue(int value) {
        this.value = value;
        return this;
    }

    public int getValue1() {
        return value1;
    }

    public RoutedBatchTestEntity withValue1(int value) {
        this.value1 = value;
        return this;
    }


}
