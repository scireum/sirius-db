/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.ComplexDelete;
import sirius.db.mixing.annotations.Length;

/**
 * Testentity for SmartQuerySpec
 */
@ComplexDelete(false)
public class SmartQueryTestChildEntity extends SQLEntity {

    @Length(50)
    private String name;
    public static final Mapping NAME = Mapping.named("name");

    private final SQLEntityRef<SmartQueryTestParentEntity> parent =
            SQLEntityRef.on(SmartQueryTestParentEntity.class, SQLEntityRef.OnDelete.CASCADE);
    public static final Mapping PARENT = Mapping.named("parent");

    private final SQLEntityRef<SmartQueryTestParentEntity> otherParent =
            SQLEntityRef.on(SmartQueryTestParentEntity.class, SQLEntityRef.OnDelete.CASCADE);
    public static final Mapping OTHER_PARENT = Mapping.named("otherParent");

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SQLEntityRef<SmartQueryTestParentEntity> getParent() {
        return parent;
    }

    public SQLEntityRef<SmartQueryTestParentEntity> getOtherParent() {
        return otherParent;
    }
}
