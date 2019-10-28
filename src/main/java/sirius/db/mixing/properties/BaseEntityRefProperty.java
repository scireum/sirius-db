/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import sirius.db.jdbc.OMA;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.annotations.ComplexDelete;
import sirius.db.mixing.types.BaseEntityRef;
import sirius.kernel.Sirius;
import sirius.kernel.async.TaskContext;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.commons.Watch;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.nls.NLS;

import java.lang.reflect.Field;
import java.util.Optional;

/**
 * Base implementation for handling properties of type {@link BaseEntityRef}.
 *
 * @param <I> the type of the primary key
 * @param <E> the type of entities being referenced
 * @param <R> the type of the reference itself
 */
public abstract class BaseEntityRefProperty<I, E extends BaseEntity<I>, R extends BaseEntityRef<I, E>>
        extends Property {

    private static final String PARAM_TYPE = "type";
    private static final String PARAM_OWNER = "owner";
    private static final String PARAM_FIELD = "field";
    private static final String PARAM_SOURCE = "source";
    private static final String PARAM_COUNT = "count";

    @Part
    protected static Mixing mixing;

    protected R entityRef;
    protected Class<? extends BaseEntity<?>> referencedType;
    protected EntityDescriptor referencedDescriptor;

    protected BaseEntityRefProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    /**
     * Returns the entity class of the referenced type.
     *
     * @return the class of the referenced entity
     */
    public Class<? extends BaseEntity<?>> getReferencedType() {
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
            referencedDescriptor = mixing.getDescriptor(entityRef.getType());
        }

        return referencedDescriptor;
    }

    private R getReferenceEntityRef() {
        if (entityRef == null) {
            this.entityRef = getEntityRef(accessPath.apply(descriptor.getReferenceInstance()));
        }

        return entityRef;
    }

    @Override
    protected Object getValueFromField(Object target) {
        return getEntityRef(target).getId();
    }

    @Override
    public Object transformValue(Value value) {
        if (value.isEmptyString()) {
            return null;
        }

        if (entityRef.getType().isInstance(value.get())) {
            return value.get();
        }

        Optional<E> e = find(entityRef.getType(), value);
        if (!e.isPresent()) {
            throw illegalFieldValue(value);
        }
        return e.get();
    }

    @Override
    protected Object transformFromDatasource(Class<? extends BaseMapper<?, ?, ?>> mapperType, Value object) {
        return object.get();
    }

    @Override
    protected Object transformToDatasource(Class<? extends BaseMapper<?, ?, ?>> mapperType, Object object) {
        return object;
    }

    /**
     * Actually resolves the the type and value into an entity.
     *
     * @param type  the type to resolve
     * @param value the id to resolve
     * @return the referenced entity wrapped as optional or an empty optional if no entity was found
     */
    protected abstract Optional<E> find(Class<E> type, Value value);

    @SuppressWarnings("unchecked")
    protected R getEntityRef(Object entity) {
        try {
            return (R) super.getValueFromField(entity);
        } catch (Exception e) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .error(e)
                            .withSystemErrorMessage(
                                    "Unable to obtain EntityRef object from entity ref field ('%s' in '%s'): %s (%s)",
                                    getName(),
                                    descriptor.getType().getName())
                            .handle();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void setValueToField(Object value, Object target) {
        R ref = getEntityRef(target);
        if (value == null || value instanceof BaseEntity<?>) {
            ref.setValue((E) value);
        } else {
            ref.setId((I) value);
        }
    }

    @Override
    protected void onBeforeSaveChecks(Object entity) {
        R ref = getEntityRef(accessPath.apply(entity));
        if (ref.containsNonpersistentValue()) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .withSystemErrorMessage(
                                    "Cannot save '%s' (%s) because the referenced entity '%s' in '%s' was not persisted yet.",
                                    entity,
                                    entity.getClass().getName(),
                                    ref.getValueIfPresent().orElse(null),
                                    getName())
                            .handle();
        }

        if (!BaseEntity.class.isAssignableFrom(entity.getClass())) {
            return;
        }

        BaseEntity<?> baseEntity = (BaseEntity<?>) entity;
        if (!baseEntity.isNew() && ref.hasWriteOnceSemantics() && baseEntity.isChanged(nameAsMapping)) {
            throw Exceptions.handle()
                            .to(OMA.LOG)
                            .withSystemErrorMessage(
                                    "Cannot save '%s' (%s) because the property '%s' has write once semantics but was changed!",
                                    entity,
                                    entity.getClass().getName(),
                                    getName())
                            .handle();
        }
    }

    @Override
    public void link() {
        super.link();

        BaseEntityRef.OnDelete deleteHandler = getReferenceEntityRef().getDeleteHandler();
        if (deleteHandler != BaseEntityRef.OnDelete.IGNORE) {
            if (!BaseEntity.class.isAssignableFrom(descriptor.getType())) {
                Mixing.LOG.WARN("Error in property % for %s is not a subclass of BaseEntity."
                                + "The only supported DeleteHandler is IGNORE!.", this, getDescriptor());
                return;
            }

            // If a cascade delete handler is present and the referenced entity is not explicitely marked as
            // "non complex" and we're within the IDE or running as a test, we force the system to compute / lookup
            // the associated NLS keys which might be required to generated appropriate deletion logs or rejection
            // errors. (Otherwise this might be missed while developing or testing the system..)
            if (getReferencedDescriptor().getAnnotation(ComplexDelete.class).map(ComplexDelete::value).orElse(true)
                || deleteHandler == BaseEntityRef.OnDelete.REJECT) {
                if (Sirius.isDev() || Sirius.isStartedAsTest()) {
                    getDescriptor().getPluralLabel();
                    getReferencedDescriptor().getLabel();
                    getLabel();
                    getFullLabel();
                }
            }
        }

        if (deleteHandler == BaseEntityRef.OnDelete.CASCADE) {
            getReferencedDescriptor().addCascadeDeleteHandler(this::onDeleteCascade);
        } else if (deleteHandler == BaseEntityRef.OnDelete.SET_NULL) {
            getReferencedDescriptor().addCascadeDeleteHandler(this::onDeleteSetNull);
        } else if (deleteHandler == BaseEntityRef.OnDelete.REJECT) {
            getReferencedDescriptor().addBeforeDeleteHandler(this::onDeleteReject);
        }
    }

    protected void onDeleteSetNull(Object e) {
        TaskContext taskContext = TaskContext.get();
        taskContext.smartLogLimited(() -> NLS.fmtr("BaseEntityRefProperty.cascadeSetNull")
                                             .set(PARAM_TYPE, getDescriptor().getPluralLabel())
                                             .set(PARAM_OWNER, Strings.limit(e, 30))
                                             .set(PARAM_FIELD, getLabel())
                                             .format());

        BaseEntity<?> referenceInstance = (BaseEntity<?>) getDescriptor().getReferenceInstance();
        referenceInstance.getMapper()
                         .select(referenceInstance.getClass())
                         .eq(nameAsMapping, ((BaseEntity<?>) e).getId())
                         .iterateAll(other -> cascadeSetNull(taskContext, other));
    }

    private void cascadeSetNull(TaskContext taskContext, BaseEntity<?> other) {
        Watch watch = Watch.start();
        setValue(other, null);
        other.getMapper().update(other);
        taskContext.addTiming(NLS.get("BaseEntityRefProperty.cascadedSetNull"), watch.elapsedMillis());
    }

    protected void onDeleteCascade(Object e) {
        TaskContext taskContext = TaskContext.get();

        taskContext.smartLogLimited(() -> NLS.fmtr("BaseEntityRefProperty.cascadeDelete")
                                             .set(PARAM_TYPE, getDescriptor().getPluralLabel())
                                             .set(PARAM_OWNER, Strings.limit(e, 30))
                                             .set(PARAM_FIELD, getLabel())
                                             .format());

        BaseEntity<?> referenceInstance = (BaseEntity<?>) getDescriptor().getReferenceInstance();
        referenceInstance.getMapper()
                         .select(referenceInstance.getClass())
                         .eq(nameAsMapping, ((BaseEntity<?>) e).getId())
                         .iterateAll(other -> cascadeDelete(taskContext, other));
    }

    private void cascadeDelete(TaskContext taskContext, BaseEntity<?> other) {
        Watch watch = Watch.start();
        other.getMapper().delete(other);
        taskContext.addTiming(NLS.get("BaseEntityRefProperty.cascadedDelete"), watch.elapsedMillis());
    }

    protected void onDeleteReject(Object e) {
        BaseEntity<?> referenceInstance = (BaseEntity<?>) getDescriptor().getReferenceInstance();
        long count = referenceInstance.getMapper()
                                      .select(referenceInstance.getClass())
                                      .eq(nameAsMapping, ((BaseEntity<?>) e).getId())
                                      .count();
        if (count == 1) {
            throw Exceptions.createHandled()
                            .withNLSKey("BaseEntityRefProperty.cannotDeleteEntityWithChild")
                            .set(PARAM_FIELD, getFullLabel())
                            .set(PARAM_TYPE, getReferencedDescriptor().getLabel())
                            .set(PARAM_SOURCE, getDescriptor().getLabel())
                            .handle();
        }
        if (count > 1) {
            throw Exceptions.createHandled()
                            .withNLSKey("BaseEntityRefProperty.cannotDeleteEntityWithChildren")
                            .set(PARAM_COUNT, count)
                            .set(PARAM_FIELD, getFullLabel())
                            .set(PARAM_TYPE, getReferencedDescriptor().getLabel())
                            .set(PARAM_SOURCE, getDescriptor().getLabel())
                            .handle();
        }
    }
}
