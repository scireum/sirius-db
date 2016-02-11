/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.properties;

import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.mixing.AccessPath;
import sirius.mixing.CascadeDeleteTaskQueue;
import sirius.mixing.Entity;
import sirius.mixing.EntityDescriptor;
import sirius.mixing.EntityRef;
import sirius.mixing.OMA;
import sirius.mixing.Property;
import sirius.mixing.PropertyFactory;
import sirius.mixing.Schema;
import sirius.mixing.constraints.FieldOperator;
import sirius.mixing.schema.ForeignKey;
import sirius.mixing.schema.Table;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Created by aha on 15.04.15.
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

    private EntityRef<? extends Entity> entityRef;
    private Class<? extends Entity> referencedType;
    private EntityDescriptor referencedDescriptor;

    public EntityRefProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    protected int getSQLType() {
        return Types.BIGINT;
    }

    @Part
    private static Schema schema;

    @Part
    private static CascadeDeleteTaskQueue cascadeQueue;

    @Override
    public void contributeToTable(Table table) {
        super.contributeToTable(table);
        EntityRef.OnDelete deleteHandler = getEntityRef().getDeleteHandler();
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

    public Class<? extends Entity> getReferencedType() {
        if (referencedType == null) {
            if (entityRef == null) {
                throw new IllegalStateException("Schema not linked!");
            }
            referencedType = entityRef.getType();
        }
        return referencedType;
    }

    public EntityDescriptor getReferencedDescriptor() {
        if (referencedDescriptor == null) {
            if (entityRef == null) {
                throw new IllegalStateException("Schema not linked!");
            }
            referencedDescriptor = schema.getDescriptor(entityRef.getType());
        }
        return referencedDescriptor;
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
        EntityRef.OnDelete deleteHandler = getEntityRef().getDeleteHandler();

        setTypeNameAsAlternativePropertyKey();

        createDeleteCascadeHandler(deleteHandler);

        if (deleteHandler == EntityRef.OnDelete.LAZY_CASCADE || deleteHandler == EntityRef.OnDelete.SOFT_CASCADE) {
            createBackgroundCascadeHandler();
        }
    }

    protected void setTypeNameAsAlternativePropertyKey() {
        this.alternativePropertyKey = getReferencedType().getName();
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
                if (count > 0) {
                    throw Exceptions.createHandled()
                                    .withNLSKey("EntityRefProperty.cannotDeleteEntityWithChildren")
                                    .set("count", count)
                                    .set("field", getLabel())
                                    .set("type", getReferencedDescriptor().getPluralLabel())
                                    .handle();
                }
            });
        }
    }

    protected void createBackgroundCascadeHandler() {
        cascadeQueue.addReferenceToCheck(() -> {
            try {
                oma.select(this.descriptor.getType())
                   .where(FieldOperator.on(nameAsColumn).notEqual(null))
                   .iterateAll(e -> {
                       if (!oma.select(getReferencedType()).eq(Entity.ID, getValue(e)).exists()) {
                           try {
                               oma.delete(e);
                           } catch (Throwable ex) {
                               Exceptions.handle()
                                         .to(OMA.LOG)
                                         .error(ex)
                                         .withSystemErrorMessage(
                                                 "Failed to cascade delete for '%s' (%s) via '%s': %s (%s)",
                                                 e,
                                                 descriptor.getType().getName(),
                                                 getName())
                                         .handle();
                           }
                       }
                   });
            } catch (Throwable ex) {
                Exceptions.handle()
                          .to(OMA.LOG)
                          .error(ex)
                          .withSystemErrorMessage("Failed to check for cascading deletes of '%s' via '%s': %s (%s)",
                                                  descriptor.getType().getName(),
                                                  getName())
                          .handle();
            }
        });
    }

    private EntityRef<?> getEntityRef() {
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
    protected Object transformValue(Value value) {
        if (value.isEmptyString()) {
            return null;
        }
        Optional<Entity> e = oma.find((Class<Entity>) entityRef.getType(), value.asString());
        if (!e.isPresent()) {
            throw illegalFieldValue(value);
        }
        return e.get();
    }

    protected EntityRef<?> getEntityRef(Object entity) {
        try {
            return (EntityRef<?>) super.getValueFromField(entity);
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

    @SuppressWarnings("unchecked")
    public void setReferencedEntity(Entity parent, Entity child) {
        ((EntityRef<Entity>) getEntityRef(accessPath.apply(parent))).setValue(child);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void setValueToField(Object value, Object target) {
        EntityRef<Entity> ref = (EntityRef<Entity>) super.getValueFromField(target);
        if (value == null || value instanceof Entity) {
            ref.setValue((Entity) value);
        } else {
            ref.setId((Long) value);
        }
    }
}
