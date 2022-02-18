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
import sirius.db.mixing.annotations.NullAllowed;

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

    @Length(50)
    @NullAllowed
    private String parentName;
    public static final Mapping PARENT_NAME = Mapping.named("parentName");

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

    public String getParentName() {
        return parentName;
    }

    public void setParentName(String parentName) {
        this.parentName = parentName;
    }
}
