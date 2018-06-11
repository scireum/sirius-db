/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.jdbc.SQLEntity;
import sirius.db.jdbc.schema.Schema;
import sirius.kernel.Sirius;
import sirius.kernel.di.ClassLoadAction;
import sirius.kernel.di.MutableGlobalContext;
import sirius.kernel.di.std.Framework;

import javax.annotation.Nullable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Used to add all {@link SQLEntity} classes to the {@link sirius.kernel.di.GlobalContext} so that the {@link Schema} will
 * find them.
 */
public class EntityLoadAction implements ClassLoadAction {

    private static List<Class<? extends BaseEntity<?>>> mappableClasses = new ArrayList<>();

    protected static List<Class<? extends BaseEntity<?>>> getMappableClasses() {
        return Collections.unmodifiableList(mappableClasses);
    }

    @Nullable
    @Override
    public Class<? extends Annotation> getTrigger() {
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void handle(MutableGlobalContext ctx, Class<?> clazz) throws Exception {
        if (Modifier.isAbstract(clazz.getModifiers())) {
            return;
        }

        if (!BaseEntity.class.isAssignableFrom(clazz)) {
            return;
        }

        if (!clazz.isAnnotationPresent(Framework.class)
            || Sirius.isFrameworkEnabled(clazz.getAnnotation(Framework.class).value())) {
            mappableClasses.add((Class<? extends BaseEntity<?>>) clazz);
        }
    }
}
