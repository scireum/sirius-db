/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.AfterSave;
import sirius.db.mixing.annotations.Length;
import sirius.db.mixing.annotations.Transient;

public class SQLWasCreatedTestEntity extends SQLEntity {
    public static final Mapping VALUE = Mapping.named("value");
    @Length(255)
    private String value;

    @Transient
    private boolean wasCreatedIndicator;

    @AfterSave
    protected void checkIfCreated() {
        wasCreatedIndicator = wasCreated();
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public boolean hasJustBeenCreated() {
        return wasCreatedIndicator;
    }
}
