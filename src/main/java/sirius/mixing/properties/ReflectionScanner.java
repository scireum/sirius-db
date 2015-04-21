/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.properties;

import sirius.kernel.commons.MultiMap;
import sirius.kernel.di.Injector;
import sirius.kernel.di.PartCollection;
import sirius.kernel.di.std.Parts;
import sirius.mixing.Mixable;
import sirius.mixing.OMA;
import sirius.mixing.annotations.Mixin;
import sirius.mixing.annotations.Transient;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.function.Consumer;

/**
 * Created by aha on 20.04.15.
 */
public class ReflectionScanner {

    /*
     * Contains all known property factories. These are used to transform fields defined by entity classes to
     * properties
     */
    @Parts(PropertyFactory.class)
    protected static PartCollection<PropertyFactory> factories;

    private static MultiMap<Class<? extends Mixable>, Class<?>> mixins;

    @SuppressWarnings("unchecked")
    private static Collection<Class<?>> getMixins(Class<? extends Mixable> forClass) {
        if (mixins == null) {
            MultiMap<Class<? extends Mixable>, Class<?>> mixinMap = MultiMap.create();
            for (Class<?> mixinClass : Injector.context().getParts(Mixin.class, Class.class)) {
                Class<?> target = mixinClass.getAnnotation(Mixin.class).value();
                if (Mixable.class.isAssignableFrom(target)) {
                    mixinMap.put((Class<? extends Mixable>) target, mixinClass);
                } else {
                    OMA.LOG.WARN("Mixing class '%s' has a non mixable target class (%s). Skipping mixin.",
                                 mixinClass.getName(),
                                 target.getName());
                }
            }
            mixins = mixinMap;
        }

        return mixins.get(forClass);
    }


    /*
     * Adds all properties of the given class (and its superclasses)
     */
    @SuppressWarnings("unchecked")
    public static void addFields(AccessPath accessPath,
                                 Class<?> clazz,
                                 Consumer<Property> propertyConsumer) {
       addFields(accessPath, clazz, clazz, propertyConsumer);
    }

    /*
     * Adds all properties of the given class (and its superclasses)
     */
    @SuppressWarnings("unchecked")
    private static void addFields(AccessPath accessPath,
                                 Class<?> rootClass,
                                 Class<?> clazz,
                                 Consumer<Property> propertyConsumer) {
        for (Field field : clazz.getDeclaredFields()) {
            addField(accessPath, rootClass, clazz, field, propertyConsumer);
        }

        if (Mixable.class.isAssignableFrom(clazz)) {
            for (Class<?> mixin : getMixins((Class<? extends Mixable>) clazz)) {
                addFields(expandAccessPath(mixin, accessPath), rootClass, mixin, propertyConsumer);
            }
        }

        if (clazz.getSuperclass() != null && !Object.class.equals(clazz.getSuperclass())) {
            addFields(accessPath, rootClass, clazz.getSuperclass(), propertyConsumer);
        }
    }

    private static AccessPath expandAccessPath(Class<?> mixin, AccessPath accessPath) {
        return accessPath.append(mixin.getSimpleName()+"_", obj -> ((Mixable) obj).as(mixin));
    }

    private static void addField(AccessPath accessPath,
                                 Class<?> rootClass,
                                 Class<?> clazz,
                                 Field field,
                                 Consumer<Property> propertyConsumer) {
        if (!field.isAnnotationPresent(Transient.class) && !Modifier.isStatic(field.getModifiers())) {
            for (PropertyFactory f : factories.getParts()) {
                if (f.accepts(field)) {
                    f.create(accessPath, field, propertyConsumer);
                    return;
                }
            }
            OMA.LOG.WARN("Cannot create property %s in type %s (%s)",
                         field.getName(),
                         rootClass.getName(),
                         clazz.getName());
        }
    }

}
