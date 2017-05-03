/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.mixing.annotations.Length;
import sirius.db.mixing.annotations.NullAllowed;

/**
 * Test entity with an entityRef object that may be null
 */
public class TestEntityWithNullRef extends Entity {
    @Length(50)
    private String name;
    public static final Column NAME = Column.named("name");

    public static final Column PARENT = Column.named("parent");
    @NullAllowed
    private final EntityRef<SmartQueryTestParentEntity> parent =
            EntityRef.on(SmartQueryTestParentEntity.class, EntityRef.OnDelete.CASCADE);

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public EntityRef<SmartQueryTestParentEntity> getParent() {
        return parent;
    }
}
