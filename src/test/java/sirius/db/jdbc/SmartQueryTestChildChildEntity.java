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
public class SmartQueryTestChildChildEntity extends SQLEntity {

    @Length(50)
    private String name;
    public static final Mapping NAME = Mapping.named("name");

    private final SQLEntityRef<SmartQueryTestChildEntity> parentChild =
            SQLEntityRef.on(SmartQueryTestChildEntity.class, SQLEntityRef.OnDelete.CASCADE);
    public static final Mapping PARENT_CHILD = Mapping.named("parentChild");

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SQLEntityRef<SmartQueryTestChildEntity> getParentChild() {
        return parentChild;
    }
}
