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

import javax.annotation.Nullable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Loads all {@link Nested nested objects} known by {@link Mixing}.
 */
public class NestedLoadAction implements ClassLoadAction {

    private static List<Class<? extends Nested>> mappableClasses = Collections.synchronizedList(new ArrayList<>());

    /**
     * Once a new instance is created - which only happens during framework initialization, we reset the list of known
     * entities as a new discrovery run will start.
     */
    public NestedLoadAction() {
        mappableClasses.clear();
    }

    /**
     * Contains a list of all known entity types.
     *
     * @return all known entities as discovered by the {@link sirius.kernel.Classpath}.
     */
    protected static List<Class<? extends Nested>> getMappableClasses() {
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

        if (!Nested.class.isAssignableFrom(clazz)) {
            return;
        }

        if (!clazz.isAnnotationPresent(Framework.class)
            || Sirius.isFrameworkEnabled(clazz.getAnnotation(Framework.class).value())) {
            mappableClasses.add((Class<? extends Nested>) clazz);
        }
    }
}
