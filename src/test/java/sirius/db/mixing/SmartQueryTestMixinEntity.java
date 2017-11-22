/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.mixing.annotations.Length;

public class SmartQueryTestMixinEntity extends Entity {

    @Length(50)
    private String value;
    public static final Column VALUE = Column.named("value");

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
