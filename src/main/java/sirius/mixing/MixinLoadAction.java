/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing;

import sirius.kernel.di.ClassLoadAction;
import sirius.kernel.di.MutableGlobalContext;
import sirius.mixing.annotations.Mixin;

import javax.annotation.Nullable;
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
        ctx.registerPart(clazz, Mixin.class);
    }
}
