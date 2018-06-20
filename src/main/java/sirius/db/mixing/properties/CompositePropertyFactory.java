/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import sirius.db.mixing.AccessPath;
import sirius.db.mixing.Composite;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.Property;
import sirius.db.mixing.PropertyFactory;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.function.Consumer;

/**
 * Compiles a {@link Composite} field within a {@link Mixable} into respective {@link Property}
 * instances.
 */
@Register
public class CompositePropertyFactory implements PropertyFactory {

    @Override
    public boolean accepts(EntityDescriptor descriptor, Field field) {
        return Composite.class.isAssignableFrom(field.getType());
    }

    @SuppressWarnings("unchecked")
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

        field.setAccessible(true);
        AccessPath expandedAccessPath = expandAccessPath(accessPath, field);
        EntityDescriptor.addFields(descriptor, expandedAccessPath, field.getType(), propertyConsumer);
        descriptor.addComposite((Class<? extends Composite>) field.getType());
    }

    private AccessPath expandAccessPath(AccessPath accessPath, Field field) {
        return accessPath.append(field.getName() + Mapping.SUBFIELD_SEPARATOR, obj -> {
            try {
                return field.get(obj);
            } catch (Exception e) {
                throw Exceptions.handle()
                                .to(Mixing.LOG)
                                .error(e)
                                .withSystemErrorMessage("Cannot access composite property %s in %s: %s (%s)",
                                                        field.getName(),
                                                        field.getDeclaringClass().getName())
                                .handle();
            }
        });
    }
}
