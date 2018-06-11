/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import com.google.common.collect.Maps;
import sirius.db.jdbc.SQLEntity;
import sirius.db.jdbc.OMA;
import sirius.db.mixing.annotations.Mixin;
import sirius.db.mixing.annotations.Transient;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.transformers.Composable;
import sirius.kernel.health.Exceptions;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Optional;

/**
 * A mixable can be extended by {@link Mixin}s.
 * <p>
 * As both, {@link SQLEntity} and {@link Composite} can be extended by mixins, the common functionality is kept in this
 * superclass.
 * <p>
 * This mainly utilizes the {@link Composable} framework to transform an entity or composite into a mixin of the
 * desired type.
 */
public class Mixable extends Composable {

    @Transient
    private Map<Class<?>, Object> mixins;

    @Part
    protected static OMA oma;

    @Override
    public boolean is(Class<?> type) {
        if (type.isAnnotationPresent(Mixin.class)) {
            return type.getAnnotation(Mixin.class).value().isAssignableFrom(this.getClass());
        }
        return super.is(type);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <A> Optional<A> tryAs(Class<A> adapterType) {
        if (mixins != null && mixins.containsKey(adapterType)) {
            return Optional.of((A) mixins.get(adapterType));
        }
        if (adapterType.isAnnotationPresent(Mixin.class)) {
            if (adapterType.getAnnotation(Mixin.class).value().isAssignableFrom(this.getClass())) {
                if (mixins == null) {
                    mixins = Maps.newHashMap();
                }
                try {
                    A result = makeNewInstance(adapterType);
                    mixins.put(adapterType, result);
                    return Optional.of(result);
                } catch (Exception e) {
                    throw Exceptions.handle()
                                    .to(Mixing.LOG)
                                    .error(e)
                                    .withSystemErrorMessage("Cannot create mixin '%s' for type '%s': %s (%s)"
                                                            + " - Note that a Mixin must either have a default"
                                                            + " constructor or one which takes the owner entity as"
                                                            + " first and only parameter.",
                                                            adapterType.getName(),
                                                            this.getClass().getName())
                                    .handle();
                }
            } else {
                throw Exceptions.handle()
                                .to(Mixing.LOG)
                                .withSystemErrorMessage("The requested mixin '%s' does not target '%s'",
                                                        adapterType.getName(),
                                                        this.getClass().getName())
                                .handle();
            }
        }
        return super.tryAs(adapterType);
    }

    @SuppressWarnings("unchecked")
    protected <A> A makeNewInstance(Class<A> type) throws Exception {
        Constructor<?> constructor = type.getDeclaredConstructors()[0];
        if (constructor.getParameterCount() == 1) {
            return (A) constructor.newInstance(this);
        } else {
            return (A) constructor.newInstance();
        }
    }
}
