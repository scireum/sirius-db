/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.mixing.Entity;
import sirius.mixing.annotations.Lob;

public class TestClobEntity extends Entity {

    @Lob
    private String largeValue;

    public String getLargeValue() {
        return largeValue;
    }

    public void setLargeValue(String largeValue) {
        this.largeValue = largeValue;
    }
}
