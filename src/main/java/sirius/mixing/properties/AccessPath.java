/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.properties;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * Created by aha on 15.04.15.
 */
public class AccessPath {

    private String prefix = "";
    private Function<Object, Object> accessor;

    public static AccessPath IDENTITY = new AccessPath();

    @Nonnull
    public AccessPath append(@Nonnull String prefix, @Nonnull Function<Object, Object> accessor) {
        AccessPath result = new AccessPath();
        if (this == IDENTITY) {
            result.prefix = prefix;
            result.accessor = accessor;

            return result;
        } else {
            result.prefix = this.prefix + prefix;
            result.accessor = this.accessor.andThen(accessor);
        }

        return result;
    }

    public Object apply(Object object) {
        return accessor == null ? object : accessor.apply(object);
    }

    public String prefix() {
        return prefix;
    }
}
