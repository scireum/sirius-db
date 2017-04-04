/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.mixing.annotations.Length;

/**
 * Testentity for SmartQuerySpec
 */
public class SmartQueryTestChildChildEntity extends Entity {

    @Length(50)
    private String name;
    public static final Column NAME = Column.named("name");

    private final EntityRef<SmartQueryTestChildEntity> parentChild =
            EntityRef.on(SmartQueryTestChildEntity.class, EntityRef.OnDelete.CASCADE);
    public static final Column PARENT_CHILD = Column.named("parentChild");

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public EntityRef<SmartQueryTestChildEntity> getParentChild() {
        return parentChild;
    }
}
