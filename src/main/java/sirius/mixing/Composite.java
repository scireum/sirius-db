/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing;

/**
 * Base class for all composites.
 * <p>
 * A composite can be embed in an {@link Entity}. All fields declared here will be mapped to properties
 * (and therefore columns of the entity).
 * <p>
 * As the field name containing the composite is prepended to all fields here, the same composite can be
 * used several times in one entity.
 * <p>
 * As it derives from {@link Mixable} a composite can be extended using <tt>Mixins</tt>. Also a composite can contain
 * further composites.
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2015/05
 */
public class Composite extends Mixable {
}
