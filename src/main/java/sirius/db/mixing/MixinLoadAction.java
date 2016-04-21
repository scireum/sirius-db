/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.kernel.Sirius;
import sirius.kernel.di.ClassLoadAction;
import sirius.kernel.di.MutableGlobalContext;
import sirius.kernel.di.std.Framework;
import sirius.db.mixing.annotations.Mixin;

import java.lang.annotation.Annotation;

/**
 * Created by aha on 12.03.15.
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
