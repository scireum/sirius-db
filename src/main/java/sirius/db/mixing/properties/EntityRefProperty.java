/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import sirius.db.mixing.AccessPath;
import sirius.db.mixing.CascadeDeleteTaskQueue;
import sirius.db.mixing.Entity;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.EntityRef;
import sirius.db.mixing.OMA;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.Schema;
import sirius.db.mixing.constraints.FieldOperator;
import sirius.db.mixing.schema.ForeignKey;
import sirius.db.mixing.schema.Table;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Represents a reference to another {@link Entity} field within a {@link sirius.db.mixing.Mixable}.
 */
public class EntityRefProperty extends Property {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(Field field) {
            return EntityRef.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            if (!Modifier.isFinal(field.getModifiers())) {
                OMA.LOG.WARN("Field %s in %s is not final! This will probably result in errors.",
                             field.getName(),
                             field.getDeclaringClass().getName());
            }

            propertyConsumer.accept(new EntityRefProperty(descriptor, accessPath, field));
        }
    }

    @Part
    private static Schema schema;

    @Part
    private static CascadeDeleteTaskQueue cascadeQueue;

    private EntityRef<? extends Entity> entityRef;
    private Class<? extends Entity> referencedType;
    private EntityDescriptor referencedDescriptor;

    EntityRefProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    protected int getSQLType() {
        return Types.BIGINT;
    }

    @Override
    public void contributeToTable(Table table) {
        super.contributeToTable(table);
        EntityRef.OnDelete deleteHandler = getReferenceEntityRef().getDeleteHandler();
        if (deleteHandler != EntityRef.OnDelete.SOFT_CASCADE && deleteHandler != EntityRef.OnDelete.LAZY_CASCADE) {
            ForeignKey fk = new ForeignKey();
            fk.setName("fk_"
                       + descriptor.getTableName()
                       + "_"
                       + getColumnName()
                       + "_"
                       + referencedDescriptor.getTableName());
            fk.setForeignTable(referencedDescriptor.getTableName());
            fk.addForeignColumn(1, Entity.ID.getName());
            fk.addColumn(1, getColumnName());
            table.getForeignKeys().add(fk);
        }
    }

    /**
     * Returns the entity class of the referenced type.
     *
     * @return the class of the referenced entity
     */
    public Class<? extends Entity> getReferencedType() {
        if (referencedType == null) {
            if (entityRef == null) {
                throw new IllegalStateException("Schema not linked!");
            }
            referencedType = entityRef.getType();
        }
        return referencedType;
    }

    /**
     * Returns the {@link EntityDescriptor} of the referenced entity.
     *
     * @return the referenced entity drescriptor
     */
    public EntityDescriptor getReferencedDescriptor() {
        if (referencedDescriptor == null) {
            if (entityRef == null) {
                throw new IllegalStateException("Schema not linked!");
            }
            referencedDescriptor = schema.getDescriptor(entityRef.getType());
        }
        return referencedDescriptor;
    }

    private EntityRef<?> getReferenceEntityRef() {
        if (entityRef == null) {
            this.entityRef = getEntityRef(accessPath.apply(descriptor.getReferenceInstance()));
        }
        return entityRef;
    }

    @Override
    protected Object getValueFromField(Object target) {
        return getEntityRef(target).getId();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object transformValue(Value value) {
        if (value.isEmptyString()) {
            return null;
        }
        Optional<Entity> e = oma.find((Class<Entity>) entityRef.getType(), value.asString());
        if (!e.isPresent()) {
            throw illegalFieldValue(value);
        }
        return e.get();
    }

    @SuppressWarnings("unchecked")
    protected EntityRef<Entity> getEntityRef(Object entity) {
        try {
            return (EntityRef<Entity>) super.getValueFromField(entity);
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .error(e)
                            .withSystemErrorMessage(
                                    "Unable to obtain EntityRef object from entity ref field ('%s' in '%s'): %s (%s)",
                                    getName(),
                                    descriptor.getType().getName())
                            .handle();
        }
    }

    /**
     * Updates the field ({@link EntityRef} within the given parent to point to the given child.
     * <p>
     * It also ensures that the ID is propagated correctly. If a join fetch is executed, the id property might have
     * beend skipped. This will reset the id within the EntityRef to -1, which is the placeholder of the id in the
     * partially fetched entity. Therefore, we remember the original id, which is filled via the foreign key. We then
     * apply the join-fetched value and restore the id (on both sides) if required.
     *
     * @param parent the parent containing the reference to the child
     * @param child  the referenced child entity
     */
    public void setReferencedEntity(Entity parent, Entity child) {
        EntityRef<Entity> targetRef = getEntityRef(accessPath.apply(parent));
        long referencedId = targetRef.getId() != null ? targetRef.getId() : -1;
        targetRef.setValue(child);

        // Check if the entity was partially join-fetched and restore the ID property if this
        // was not contained in the join fetch - which is quite common
        if (targetRef.getId() != referencedId && referencedId != -1) {
            child.setId(referencedId);
        }

        targetRef.setId(referencedId == -1L ? null : referencedId);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void setValueToField(Object value, Object target) {
        EntityRef<Entity> ref = getEntityRef(target);
        if (value == null || value instanceof Entity) {
            ref.setValue((Entity) value);
        } else {
            ref.setId((Long) value);
        }
    }

    @Override
    protected void onBeforeSaveChecks(Entity entity) {
        EntityRef<?> ref = getEntityRef(accessPath.apply(entity));
        if (ref.containsNonpersistentValue()) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .withSystemErrorMessage(
                                    "Cannot save '%s' (%s) because the referenced entity '%s' in '%s' has no id.",
                                    entity,
                                    entity.getClass().getName(),
                                    ref.getValue(),
                                    getName())
                            .handle();
        }
    }

    @Override
    public void link() {
        super.link();
        EntityRef.OnDelete deleteHandler = getReferenceEntityRef().getDeleteHandler();

        createDeleteCascadeHandler(deleteHandler);

        if (deleteHandler == EntityRef.OnDelete.LAZY_CASCADE || deleteHandler == EntityRef.OnDelete.SOFT_CASCADE) {
            createBackgroundCascadeHandler();
        }
    }

    protected void createDeleteCascadeHandler(EntityRef.OnDelete deleteHandler) {
        if (deleteHandler == EntityRef.OnDelete.CASCADE || deleteHandler == EntityRef.OnDelete.SOFT_CASCADE) {
            getReferencedDescriptor().addCascadeDeleteHandler(e -> oma.select(this.descriptor.getType())
                                                                      .eq(nameAsColumn, e.getId())
                                                                      .delete());
        } else if (deleteHandler == EntityRef.OnDelete.SET_NULL) {
            getReferencedDescriptor().addCascadeDeleteHandler(e -> oma.select(this.descriptor.getType())
                                                                      .eq(nameAsColumn, e.getId())
                                                                      .iterateAll(child -> {
                                                                          setValue(child, null);
                                                                          oma.update(child);
                                                                      }));
        } else if (deleteHandler == EntityRef.OnDelete.REJECT) {
            getReferencedDescriptor().addBeforeDeleteHandler(e -> {
                long count = oma.select(this.descriptor.getType()).eq(nameAsColumn, e.getId()).count();
                if (count == 1) {
                    throw Exceptions.createHandled()
                                    .withNLSKey("EntityRefProperty.cannotDeleteEntityWithChild")
                                    .set("field", getLabel())
                                    .set("type", getReferencedDescriptor().getLabel())
                                    .set("source", getDescriptor().getLabel())
                                    .handle();
                }
                if (count > 1) {
                    throw Exceptions.createHandled()
                                    .withNLSKey("EntityRefProperty.cannotDeleteEntityWithChildren")
                                    .set("count", count)
                                    .set("field", getLabel())
                                    .set("type", getReferencedDescriptor().getLabel())
                                    .set("source", getDescriptor().getLabel())
                                    .handle();
                }
            });
        }
    }

    protected void createBackgroundCascadeHandler() {
        cascadeQueue.addReferenceToCheck(this::cascadeDeletesInBackground);
    }

    private void cascadeDeletesInBackground() {
        try {
            oma.select(this.descriptor.getType()).where(FieldOperator.on(nameAsColumn).notEqual(null)).iterateAll(e -> {
                if (!oma.select(getReferencedType()).eq(Entity.ID, getValue(e)).exists()) {
                    cascadeDelete(e);
                }
            });
        } catch (Exception ex) {
            Exceptions.handle()
                      .to(OMA.LOG)
                      .error(ex)
                      .withSystemErrorMessage("Failed to check for cascading deletes of '%s' via '%s': %s (%s)",
                                              descriptor.getType().getName(),
                                              getName())
                      .handle();
        }
    }

    private void cascadeDelete(Entity e) {
        try {
            oma.delete(e);
        } catch (Exception ex) {
            Exceptions.handle()
                      .to(OMA.LOG)
                      .error(ex)
                      .withSystemErrorMessage("Failed to cascade delete for '%s' (%s) via '%s': %s (%s)",
                                              e,
                                              descriptor.getType().getName(),
                                              getName())
                      .handle();
        }
    }
}
