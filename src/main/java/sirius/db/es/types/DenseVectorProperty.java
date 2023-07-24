/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.types;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import sirius.db.es.ESPropertyInfo;
import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.kernel.commons.Json;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.function.Consumer;

/**
 * Properly handles {@link DenseVector} fields in {@linkplain sirius.db.es.ElasticEntity elastic entities}.
 */
public class DenseVectorProperty extends Property implements ESPropertyInfo {

    /**
     * Factory for generating properties based on their field type
     */
    @Register
    public static class Factory implements PropertyFactory {

        @Override
        public boolean accepts(EntityDescriptor descriptor, Field field) {
            return DenseVector.class.equals(field.getType());
        }

        @Override
        public void create(EntityDescriptor descriptor,
                           AccessPath accessPath,
                           Field field,
                           Consumer<Property> propertyConsumer) {
            DenseVectorProperty denseVectorProperty = new DenseVectorProperty(descriptor, accessPath, field);

            try {
                if (field.get(accessPath.apply(descriptor.getType().getDeclaredConstructor().newInstance())) == null) {
                    Mixing.LOG.WARN("Field %s in %s is a DenseVector. Such fields should be initialized as it"
                                    + " should never be null!", field.getName(), field.getDeclaringClass().getName());
                }

                if (!Modifier.isFinal(field.getModifiers())) {
                    Mixing.LOG.WARN("Field %s in %s is a DenseVector and should be final!",
                                    field.getName(),
                                    field.getDeclaringClass().getName());
                }
            } catch (Exception exception) {
                Mixing.LOG.WARN("An error occurred while ensuring that the field %s in %s is properly set up: %s (%s)",
                                field.getName(),
                                field.getDeclaringClass().getName(),
                                exception.getMessage(),
                                exception.getClass().getName());
            }

            propertyConsumer.accept(denseVectorProperty);
        }
    }

    /**
     * Creates a new property for the given descriptor, access path and field.
     *
     * @param descriptor the descriptor which owns the property
     * @param accessPath the access path required to obtain the target object which contains the field
     * @param field      the field which stores the database value
     */
    protected DenseVectorProperty(@Nonnull EntityDescriptor descriptor,
                                  @Nonnull AccessPath accessPath,
                                  @Nonnull Field field) {
        super(descriptor, accessPath, field);
    }

    @Override
    public void describeProperty(ObjectNode description) {
        DenseVector referenceVector = getDenseVector(getDescriptor().getReferenceInstance());
        description.put("type", "dense_vector");
        description.put("dims", referenceVector.dimensions);
        description.put("index", referenceVector.indexed);
        description.put("similarity", referenceVector.similarity.getEsName());
    }

    protected DenseVector getDenseVector(Object entity) {
        try {
            return (DenseVector) super.getValueFromField(entity);
        } catch (Exception exception) {
            throw Exceptions.handle()
                            .to(Mixing.LOG)
                            .error(exception)
                            .withSystemErrorMessage(
                                    "Unable to obtain DenseVector object from field ('%s' in '%s'): %s (%s)",
                                    getName(),
                                    descriptor.getType().getName())
                            .handle();
        }
    }

    @Override
    protected Object getValueFromField(Object target) {
        float[] vector = getDenseVector(target).vector;
        if (vector != null) {
            return vector.clone();
        }

        return null;
    }

    @Override
    protected void setValueToField(Object value, Object target) {
        getDenseVector(target).storeVector((Object[]) value);
    }

    @Override
    public Object transformValue(Value value) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Object transformFromElastic(Value object) {
        if (object.get() instanceof ArrayNode array) {
            return Json.convertToList(array, Object.class).toArray();
        }
        if (object.get() instanceof List<?> list) {
            return list.toArray();
        }
        if (object.get() instanceof Object[] array) {
            return array;
        }

        if (object.isNull()) {
            return null;
        }

        throw new IllegalArgumentException(object.getString());
    }

    @Override
    protected Object transformToElastic(Object object) {
        return object;
    }
}
