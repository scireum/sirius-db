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
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.db.mixing.annotations.ComplexDelete;
import sirius.db.mixing.types.BaseEntityRef;
import sirius.db.mixing.types.BaseEntityRefList;
import sirius.db.mongo.Mongo;
import sirius.db.mongo.MongoEntity;
import sirius.kernel.Sirius;
import sirius.kernel.async.TaskContext;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.commons.Values;
import sirius.kernel.commons.Watch;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.kernel.nls.NLS;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Represents an {@link BaseEntityRefList} field within a {@link Mixable}.
 */
public class BaseEntityRefListProperty extends Property implements ESPropertyInfo {

    private static final String PARAM_TYPE = "type";
    private static final String PARAM_OWNER = "owner";
    private static final String PARAM_FIELD = "field";
    private static final String PARAM_SOURCE = "source";
    private static final String PARAM_COUNT = "count";

    @Part
    private static Mixing mixing;

    @Part
    private static Mongo mongo;

    private BaseEntityRefList<?, ?> entityRefList;
    private EntityDescriptor referencedDescriptor;

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return BaseEntityRefList.class.isAssignableFrom(field.getType());
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

            propertyConsumer.accept(new BaseEntityRefListProperty(descriptor, accessPath, field));
        }
    }

    BaseEntityRefListProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    protected BaseEntityRefList<?, ?> getEntityRefList(Object entity) {
        try {
            return (BaseEntityRefList<?, ?>) super.getValueFromField(entity);
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

    @Override
    protected Object getValueFromField(Object target) {
        return getEntityRefList(target).data();
    }

    @Override
    public Object getValueAsCopy(Object entity) {
        return getEntityRefList(accessPath.apply(entity)).copyList();
    }

    @Override
    public Object transformValue(Value value) {
        if (value.isEmptyString()) {
            return null;
        }

        return value.get();
    }

    @Override
    protected Object transformToElastic(Object object) {
        return object;
    }

    @Override
    protected Object transformToMongo(Object object) {
        return object;
    }

    @Override
    protected Object transformFromElastic(Value object) {
        return object.get();
    }

    @Override
    protected Object transformFromMongo(Value object) {
        return object.get();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void setValueToField(Object value, Object target) {
        getEntityRefList(target).setData((List<String>) value);
    }

    @Override
    public void describeProperty(JSONObject description) {
        description.put("type", "keyword");
        transferOption(IndexMappings.MAPPING_STORED, getAnnotation(IndexMode.class), IndexMode::stored, description);
        transferOption(IndexMappings.MAPPING_INDEX, getAnnotation(IndexMode.class), IndexMode::indexed, description);
        transferOption(IndexMappings.MAPPING_DOC_VALUES,
                       getAnnotation(IndexMode.class),
                       IndexMode::docValues,
                       description);
    }

    @Override
    public void parseValues(Object e, Values values) {
        List<String> stringData = new ArrayList<>();
        for (int i = 0; i < values.length(); i++) {
            values.at(i).ifFilled(value -> stringData.add(value.toString()));
        }
        setValue(e, stringData);
    }

    protected BaseEntityRefList<?, ?> getReferenceEntityRefList() {
        if (entityRefList == null) {
            this.entityRefList = getEntityRefList(accessPath.apply(descriptor.getReferenceInstance()));
        }

        return entityRefList;
    }

    /**
     * Returns the {@link EntityDescriptor} of the referenced entity.
     *
     * @return the referenced entity drescriptor
     */
    protected EntityDescriptor getReferencedDescriptor() {
        if (referencedDescriptor == null) {
            referencedDescriptor = mixing.getDescriptor(getReferenceEntityRefList().getType());
        }

        return referencedDescriptor;
    }

    @Override
    public void link() {
        super.link();

        BaseEntityRef.OnDelete deleteHandler = getReferenceEntityRefList().getDeleteHandler();
        if (deleteHandler != BaseEntityRef.OnDelete.IGNORE) {
            if (!BaseEntityRefProperty.ensureProperReferenceType(this, getReferencedDescriptor())) {
                return;
            }

            BaseEntityRefProperty.ensureLabelsArePresent(this, referencedDescriptor, deleteHandler);
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

        Object idBeingDeleted = ((BaseEntity<?>) e).getId();
        if (referenceInstance instanceof MongoEntity) {
            // MongoDB provides a fast and efficient way of removing and ID from a list...
            mongo.update()
                 .where(nameAsMapping, idBeingDeleted)
                 .pull(nameAsMapping, idBeingDeleted)
                 .executeFor(referenceInstance.getClass());
        } else {
            referenceInstance.getMapper()
                             .select(referenceInstance.getClass())
                             .eq(nameAsMapping, idBeingDeleted)
                             .iterateAll(other -> cascadeSetNull(taskContext, idBeingDeleted, other));
        }
    }

    private void cascadeSetNull(TaskContext taskContext, Object idBeingDeleted, BaseEntity<?> other) {
        Watch watch = Watch.start();
        getEntityRefList(accessPath.apply(other)).modify().remove(idBeingDeleted);
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
