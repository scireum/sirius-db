/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;

import java.util.Objects;

/**
 * Base class for all nested objects.
 * <p>
 * A nested object can be placed in a {@link sirius.db.mixing.types.NestedList} or
 * {@link sirius.db.mixing.types.StringNestedMap} if the {@link BaseMapper mapper} supports it.
 * <p>
 * Do not use this to embed a single object in an entity as a {@link Composite} can be used for that. This
 * will automatically unfold all properties along with consistency checks.
 * <p>
 * As it derives from {@link Mixable} a nested object can be extended using <tt>Mixins</tt>.
 */
public class Nested extends Mixable {

    @Part
    private static Mixing mixing;

    public Nested copy() {
        try {
            EntityDescriptor descriptor = mixing.getDescriptor(getClass());

            Nested copy = getClass().newInstance();

            for (Property property : descriptor.getProperties()) {
                property.setValue(copy, property.getValueAsCopy(this));
            }

            return copy;
        } catch (Exception e) {
            throw Exceptions.handle(Mixing.LOG, e);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        if (getClass().isInstance(obj)) {
            return false;
        }

        EntityDescriptor descriptor = mixing.getDescriptor(getClass());
        for (Property property : descriptor.getProperties()) {
            if (!Objects.equals(property.getValue(this), property.getValue(obj))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = 1;

        EntityDescriptor descriptor = mixing.getDescriptor(getClass());
        for (Property property : descriptor.getProperties()) {
            Object element = property.getValue(this);
            result = 31 * result + (element == null ? 0 : element.hashCode());
        }

        return result;
    }
}
