/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.Length;

/**
 * Testentity for SmartQuerySpec
 */
public class SmartQueryTestParentEntity extends SQLEntity {

    @Length(50)
    private String name;
    public static final Mapping NAME = Mapping.named("name");

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
