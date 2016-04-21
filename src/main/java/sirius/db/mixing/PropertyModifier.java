/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Used to modify / update a property before it is finally added to the descriptor.
 * <p>
 * Implementations of this interface can be registered (using {@link sirius.kernel.di.std.Register}) and will be
 * discovered and applied by {@link EntityDescriptor}. They can be used to fine-tune properties.
 * <p>
 * This is mostly useful for customer customizations ({@link sirius.kernel.Sirius#getActiveConfigurations()}.
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2015/05
 */
public interface PropertyModifier {

    /**
     * Returns the target type which properties should be fine tuned.
     *
     * @return the target type which properties should be modified. Returns <tt>null</tt> to apply to all types.
     */
    @Nullable
    Class<?> targetType();

    /**
     * Returns the name of the field to fine tune.
     *
     * @return the name of the field to modify. Returns <tt>null</tt> to modify all fields.
     */
    @Nullable
    String targetFieldName();

    /**
     * Invoked to all properties which match the filters set by <tt>targetType()</tt> and <tt>targetFieldName()</tt>.
     *
     * @param property the property to modified
     * @return a modified or replaced version of the given property
     */
    @Nonnull
    Property modify(@Nonnull Property property);
}
