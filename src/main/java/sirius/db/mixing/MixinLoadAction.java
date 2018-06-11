/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.mixing.annotations.Mixin;
import sirius.kernel.Sirius;
import sirius.kernel.di.ClassLoadAction;
import sirius.kernel.di.MutableGlobalContext;
import sirius.kernel.di.std.Framework;

import java.lang.annotation.Annotation;

/**
 * Loads all {@link Mixin}s so that they don't have to wear a {@link sirius.kernel.di.std.Register} annotation.
 */
public class MixinLoadAction implements ClassLoadAction {

    @Override
    public Class<? extends Annotation> getTrigger() {
        return Mixin.class;
    }

    @Override
    public void handle(MutableGlobalContext ctx, Class<?> clazz) throws Exception {
        if (clazz.isAnnotationPresent(Framework.class)) {
            if (Sirius.isFrameworkEnabled(clazz.getAnnotation(Framework.class).value())) {
                ctx.registerPart(clazz, Mixin.class);
            }
        } else {
            ctx.registerPart(clazz, Mixin.class);
        }
    }
}
