/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import com.alibaba.fastjson.JSONObject;
import sirius.db.es.ESPropertyInfo;
import sirius.db.es.IndexMappings;
import sirius.db.es.annotations.IndexMode;
import sirius.db.jdbc.OMA;
import sirius.db.jdbc.SQLEntity;
import sirius.db.jdbc.SQLEntityRef;
import sirius.db.jdbc.schema.ForeignKey;
import sirius.db.jdbc.schema.SQLPropertyInfo;
import sirius.db.jdbc.schema.Table;
import sirius.db.jdbc.schema.TableColumn;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Represents a reference to another {@link SQLEntity} field within a {@link Mixable}.
 */
public class SQLEntityRefProperty extends BaseEntityRefProperty<Long, SQLEntity, SQLEntityRef<SQLEntity>>
        implements SQLPropertyInfo, ESPropertyInfo {

    private static final int MAX_FOREIGN_KEY_NAME_LENGTH = 60;

    @Part
    private static OMA oma;

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return SQLEntityRef.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            if (!Modifier.isFinal(field.getModifiers())) {
                Mixing.LOG.WARN("Field %s in %s is not final! This will probably result in errors.",
                                field.getName(),
                                field.getDeclaringClass().getName());
            }

            propertyConsumer.accept(new SQLEntityRefProperty(descriptor, accessPath, field));
        }
    }

    protected SQLEntityRefProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    public void contributeToTable(Table table) {
        table.getColumns().add(new TableColumn(this, Types.BIGINT));

        if (shouldCreateForeignKeyConstraint()) {
            ForeignKey fk = new ForeignKey();
            fk.setName(computeForeignKeyName());
            fk.setForeignTable(getReferencedDescriptor().getRelationName());
            fk.addForeignColumn(1, SQLEntity.ID.getName());
            fk.addColumn(1, getPropertyName());
            table.getForeignKeys().add(fk);
        }
    }

    private boolean shouldCreateForeignKeyConstraint() {
        if (!SQLEntity.class.isAssignableFrom(getReferencedType())) {
            return false;
        }
        if (!Strings.areEqual(getDescriptor().getRealm(), getReferencedDescriptor().getRealm())) {
            return false;
        }
        return !entityRef.isWeak();
    }

    private String computeForeignKeyName() {
        String result = "fk_"
                        + descriptor.getRelationName()
                        + "_"
                        + getPropertyName()
                        + "_"
                        + referencedDescriptor.getRelationName();

        if (result.length() < MAX_FOREIGN_KEY_NAME_LENGTH) {
            return result;
        }

        return Strings.limit("fk_" + descriptor.getRelationName() + "_" + getPropertyName(),
                             MAX_FOREIGN_KEY_NAME_LENGTH,
                             false);
    }

    @Override
    public void describeProperty(JSONObject description) {
        description.put(IndexMappings.MAPPING_TYPE, "long");
        transferOption(IndexMappings.MAPPING_STORED, getAnnotation(IndexMode.class), IndexMode::stored, description);
        transferOption(IndexMappings.MAPPING_INDEX, getAnnotation(IndexMode.class), IndexMode::indexed, description);
        transferOption(IndexMappings.MAPPING_DOC_VALUES,
                       getAnnotation(IndexMode.class),
                       IndexMode::docValues,
                       description);
    }

    @Override
    protected Optional<SQLEntity> find(Class<SQLEntity> type, Value value) {
        return oma.find(type, value.get());
    }

    /**
     * Updates the field ({@link SQLEntityRef} within the given parent to point to the given child.
     * <p>
     * It also ensures that the ID is propagated correctly. If a join fetch is executed, the id property might have
     * beend skipped. This will reset the id within the EntityRef to -1, which is the placeholder of the id in the
     * partially fetched entity. Therefore, we remember the original id, which is filled via the foreign key. We then
     * apply the join-fetched value and restore the id (on both sides) if required.
     *
     * @param parent the parent containing the reference to the child
     * @param child  the referenced child entity
     */
    public void setReferencedEntity(SQLEntity parent, SQLEntity child) {
        SQLEntityRef<SQLEntity> targetRef = getEntityRef(accessPath.apply(parent));
        long referencedId = targetRef.getId() != null ? targetRef.getId() : -1;
        targetRef.setValue(child);

        // Check if the entity was partially join-fetched and restore the ID property if this
        // was not contained in the join fetch - which is quite common
        if (!targetRef.is(referencedId) && referencedId != -1) {
            child.setId(referencedId);
            targetRef.setId(referencedId);
        }
    }
}
