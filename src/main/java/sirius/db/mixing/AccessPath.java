/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.kernel.commons.Strings;

import javax.annotation.Nonnull;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Determines how to access the field of a property within the entity.
 * <p>
 * As properties can be contained within composites or mixins (or a combination thereof) we may need to
 * fetch the composite or use {@link Mixable#as(Class)} to fetch the mixin in order to access the field. The types
 * and number of calls are defined by an <tt>AccessPath</tt>.
 */
public class AccessPath {

    private String prefix = "";
    private Function<Object, Object> accessor;

    /**
     * Represents a NO-OP access path which is used for all fields which are directly contained in the entity.
     */
    public static final AccessPath IDENTITY = new AccessPath();

    /**
     * Creates a new access path which appends the given prefix and accessor to the current access path.
     *
     * @param prefixToAppend the prefix to be appended to all field names to make them unique (composites might be
     *                       embedded twice).
     * @param accessor       the function used to access the sub entity (composite or mixin) from the current
     *                       access path
     * @return a new access path which is extended by the given prefix and accessor
     */
    @Nonnull
    public AccessPath append(@Nonnull String prefixToAppend, @Nonnull UnaryOperator<Object> accessor) {
        AccessPath result = new AccessPath();
        if (IDENTITY.equals(this)) {
            result.prefix = prefixToAppend;
            result.accessor = accessor;

            return result;
        }

        result.prefix = qualify(prefixToAppend);
        result.accessor = this.accessor.andThen(accessor);

        return result;
    }

    /**
     * Qualifies the given field name by prepending the access path represented by this instance.
     *
     * @param name the field name to access.
     * @return the fully qualified access path (e.g. [composite]_[name]).
     */
    public String qualify(String name) {
        if (Strings.isEmpty(prefix)) {
            return name;
        }

        return prefix + Mapping.SUBFIELD_SEPARATOR + name;
    }

    /**
     * Applies the access path of the given object.
     * <p>
     * Applies all accessor functions one after another to extract the object containing a property from the given
     * root object.
     *
     * @param object the root object from which the designated target object (composite or mixin) is to be extracted
     * @return the designated target which contains the field of a property (a composite or mixin)
     */
    @Nonnull
    public Object apply(@Nonnull Object object) {
        return accessor == null ? object : accessor.apply(object);
    }

    /**
     * Returns the prefix appended to all fields to generate unique property names.
     *
     * @return the prefix used to represent this access path
     */
    public String prefix() {
        return prefix;
    }
}
