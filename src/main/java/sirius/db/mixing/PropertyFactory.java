/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.kernel.di.std.Priorized;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.util.function.Consumer;

/**
 * Describes a property factory which generates a {@link Property} for a given {@link Field}.
 * <p>
 * When scanning a class to compute its {@link EntityDescriptor}, for each field each
 * <tt>PropertyFactory</tt> is queried. The first to return <tt>true</tt> as result of
 * {@link #accepts(EntityDescriptor, Field)} will be used to compute the property for a field by calling
 * {@link #create(EntityDescriptor, AccessPath, Field, Consumer)}.
 */
public interface PropertyFactory extends Priorized {

    @Override
    default int getPriority() {
        return DEFAULT_PRIORITY;
    }

    /**
     * Determines if the given field is handled by this property factory.
     *
     * @param descriptor the current descriptor
     * @param field      the field to create a property from
     * @return <tt>true</tt> if the factory can create a property for the given field, <tt>false</tt> otherwise
     */
    boolean accepts(EntityDescriptor descriptor, @Nonnull Field field);

    /**
     * Computes a {@link Property} for the given field.
     *
     * @param descriptor       the descriptor to which the property will belong
     * @param accessPath       the accesspath used to reach the given property
     * @param field            the field to create a property from
     * @param propertyConsumer the consumer which is used to process created properties
     */
    void create(@Nonnull EntityDescriptor descriptor,
                @Nonnull AccessPath accessPath,
                @Nonnull Field field,
                @Nonnull Consumer<Property> propertyConsumer);
}
