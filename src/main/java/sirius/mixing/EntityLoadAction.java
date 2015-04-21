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

import javax.annotation.Nullable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;

/**
 * Created by aha on 12.03.15.
 */
public class EntityLoadAction implements ClassLoadAction {
    @Nullable
    @Override
    public Class<? extends Annotation> getTrigger() {
        return null;
    }

    @Override
    public void handle(MutableGlobalContext ctx, Class<?> clazz) throws Exception {
        if (!Modifier.isAbstract(clazz.getModifiers()) && Entity.class.isAssignableFrom(clazz)) {
            ctx.registerPart(clazz.newInstance(), Entity.class);
        }
    }
}
