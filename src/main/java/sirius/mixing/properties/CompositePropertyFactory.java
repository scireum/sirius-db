/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.properties;

import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;
import sirius.mixing.Composite;
import sirius.mixing.OMA;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by aha on 15.04.15.
 */
@Register
public class CompositePropertyFactory implements PropertyFactory {

    @Override
    public boolean accepts(Field field) {
        return Composite.class.isAssignableFrom(field.getType());
    }

    @Override
    public void create(AccessPath accessPath,
                       Field field,
                       Consumer<Property> propertyConsumer) {
        if (!Modifier.isFinal(field.getModifiers())) {
            OMA.LOG.WARN("Field %s in %s is not final! This will probably result in errors.", field.getName(), field.getDeclaringClass().getName());
        }

        field.setAccessible(true);
        accessPath = expandAccessPath(accessPath, field);
        ReflectionScanner.addFields(accessPath, field.getType(), propertyConsumer);
    }

    private AccessPath expandAccessPath(AccessPath accessPath, Field field) {
        accessPath = accessPath.append(field.getName() + "_",obj -> {
            try {
                return field.get(obj);
            } catch (Throwable e) {
                throw Exceptions.handle()
                                .to(OMA.LOG)
                                .error(e)
                                .withSystemErrorMessage("Cannot access composite property %s in %s: %s (%s)",
                                                        field.getName(),
                                                        field.getDeclaringClass().getName())
                                .handle();
            }
        });
        return accessPath;
    }
}
