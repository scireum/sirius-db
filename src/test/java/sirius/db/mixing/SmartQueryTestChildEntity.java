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
public class SmartQueryTestChildEntity extends Entity {

    @Length(length = 50)
    private String name;
    public static final Column NAME = Column.named("name");

    private final EntityRef<SmartQueryTestParentEntity> parent = EntityRef.on(SmartQueryTestParentEntity.class,
                                                                              EntityRef.OnDelete.CASCADE);
    public static final Column PARENT = Column.named("parent");

    private final EntityRef<SmartQueryTestParentEntity> otherParent = EntityRef.on(SmartQueryTestParentEntity.class,
                                                                                   EntityRef.OnDelete.CASCADE);
    public static final Column OTHER_PARENT = Column.named("otherParent");

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public EntityRef<SmartQueryTestParentEntity> getParent() {
        return parent;
    }

    public EntityRef<SmartQueryTestParentEntity> getOtherParent() {
        return otherParent;
    }
}
