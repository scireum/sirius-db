/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing;

import com.google.common.collect.Maps;
import sirius.kernel.di.morphium.Adaptable;
import sirius.kernel.health.Exceptions;
import sirius.mixing.annotations.Mixin;
import sirius.mixing.annotations.Transient;

import java.util.Map;
import java.util.Optional;

/**
 * Created by aha on 20.04.15.
 */
public class Mixable implements Adaptable {

    @Transient
    private Map<Class<?>, Object> mixins;

    @Override
    public boolean is(Class<?> type) {
        if (type.isAnnotationPresent(Mixin.class)) {
            return type.getAnnotation(Mixin.class).value().isAssignableFrom(this.getClass());
        }
        return Adaptable.super.is(type);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <A> Optional<A> tryAs(Class<A> adapterType) {
        synchronized (this) {
            if (mixins != null && mixins.containsKey(adapterType)) {
                return Optional.of((A) mixins.get(adapterType));
            }
            if (adapterType.isAnnotationPresent(Mixin.class)) {
                if (adapterType.getAnnotation(Mixin.class).value().isAssignableFrom(this.getClass())) {
                    if (mixins == null) {
                        mixins = Maps.newHashMap();
                    }
                    try {
                        A result = adapterType.newInstance();
                        mixins.put(adapterType, result);
                        return Optional.of(result);
                    } catch (Throwable e) {
                        throw Exceptions.handle()
                                        .to(OMA.LOG)
                                        .error(e)
                                        .withSystemErrorMessage("Cannot create mixin '%s' for type '%s': %s (%s)",
                                                                adapterType.getName(),
                                                                this.getClass().getName())
                                        .handle();
                    }
                } else {
                    throw Exceptions.handle()
                                    .to(OMA.LOG)
                                    .withSystemErrorMessage("The requested mixin '%s' does not target '%s'",
                                                            adapterType.getName(),
                                                            this.getClass().getName())
                                    .handle();
                }
            }
            return Adaptable.super.tryAs(adapterType);
        }
    }
}
