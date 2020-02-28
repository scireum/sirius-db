/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.AfterSave;
import sirius.db.mixing.annotations.Transient;

public class MangoWasCreatedTestEntity extends MongoEntity {

    public static final Mapping VALUE = Mapping.named("value");
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
