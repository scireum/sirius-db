/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.Versioned;

@Versioned
public class QueryTestEntity extends ElasticEntity {

    public static final Mapping VALUE = Mapping.named("value");
    private String value;

    public static final Mapping COUNTER = Mapping.named("counter");
    private int counter;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getCounter() {
        return counter;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }
}
