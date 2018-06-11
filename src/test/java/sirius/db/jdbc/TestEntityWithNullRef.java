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
import sirius.db.mixing.annotations.NullAllowed;

/**
 * Test entity with an entityRef object that may be null
 */
public class TestEntityWithNullRef extends SQLEntity {
    @Length(50)
    private String name;
    public static final Mapping NAME = Mapping.named("name");

    public static final Mapping PARENT = Mapping.named("parent");
    @NullAllowed
    private final SQLEntityRef<SmartQueryTestParentEntity> parent =
            SQLEntityRef.on(SmartQueryTestParentEntity.class, SQLEntityRef.OnDelete.CASCADE);

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SQLEntityRef<SmartQueryTestParentEntity> getParent() {
        return parent;
    }
}
