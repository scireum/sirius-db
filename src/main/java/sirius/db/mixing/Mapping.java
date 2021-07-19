/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.kernel.commons.Strings;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Represents a column (property) name which is used in queries.
 * <p>
 * For each field, a <tt>Mapping</tt> with the same name must be defined. This mapping is used to reference the
 * field (or its property) in queries. This adds syntactic checks and permits refactorings (renaming etc.).
 * <p>
 * An example for a field with its mapping would be:
 * <pre>
 * {@code
 *    public static final Mapping AGE = Mapping.named("age");
 *    private int age;
 * }
 * </pre>
 */
public class Mapping {

    /**
     * Used to join several field names (e.g. for composites or mixins).
     */
    public static final String SUBFIELD_SEPARATOR = "_";

    /**
     * Used to join several field names (e.g. for nesteds).
     */
    public static final String NESTED_SEPARATOR = ".";

    /**
     * Contains the name of the represented field
     */
    private final String name;

    /**
     * Contains the parent in case this field resides in a composite or mixin
     */
    private final Mapping parent;

    /*
     * Creates a new mapping. Use named(String) to create a new constant within a class. Use inner(Mapping) or
     * join(Mapping) to access composites, mixins or referenced entities.
     */
    private Mapping(String name, Mapping parent) {
        this.name = name;
        this.parent = parent;
    }

    /**
     * Creates a new <tt>Mapping</tt>. This should be used to create a <tt>public static final</tt> constant
     * in the class where the field is defined.
     *
     * @param name the name of the represented field
     * @return a mapping representing the field with the given name
     */
    public static Mapping named(String name) {
        return new Mapping(name, null);
    }

    /**
     * Creates a new <tt>Mapping</tt> for a mixin class.
     *
     * @param mixinType the class which defines the mixin
     * @return a mapping representing the mixin
     */
    public static Mapping mixin(Class<?> mixinType) {
        return new Mapping(mixinType.getSimpleName(), null);
    }

    /**
     * References an inner field of a composite represented by this mapping.
     *
     * @param inner the inner field of the composite represented by this mapping
     * @return a mapping representing the combined path of this mapping and inner field
     */
    public Mapping inner(Mapping inner) {
        return new Mapping(name + SUBFIELD_SEPARATOR + inner.name, null);
    }

    /**
     * References a field of a nested list or map represented by this mapping.
     * <p>
     * This is the equivalent of {@link #join(Mapping)} for NOSQL databases like Elasticsearch or Mongo DB.
     *
     * @param inner the inner field of the nested represented by this mapping
     * @return a mapping representing the combined path of this mapping and inner field
     */
    public Mapping nested(Mapping inner) {
        return new Mapping(name + NESTED_SEPARATOR + inner.name, null);
    }

    /**
     * References a dynamic inner property of a nested map represented by this mapping.
     * <p>
     * This is the equivalent of {@link #join(Mapping)} for NOSQL databases like Elasticsearch or Mongo DB.
     *
     * @param inner the inner field of the nested represented by this mapping
     * @return a mapping representing the combined path of this mapping and inner field
     */
    public Mapping nested(String inner) {
        return new Mapping(name + NESTED_SEPARATOR + inner, null);
    }

    /**
     * References a mixin for an inner composite or referenced type.
     *
     * @param mixinType t the class which defines the mixin
     * @return a mapping representing the combined path of this mapping and the given mixin
     */
    public Mapping inMixin(Class<?> mixinType) {
        return new Mapping(mixinType.getSimpleName() + SUBFIELD_SEPARATOR + name, null);
    }

    /**
     * Joins the referenced field described by <tt>joinMapping</tt>.
     * <p>
     * Note that this mapping needs to represent an <tt>EntityRef</tt> field.
     *
     * @param joinMapping the mapping of the referenced entity to join
     * @return a mapping which joins the entity represented by this mapping and accesses the given <tt>joinMapping</tt>
     */
    public Mapping join(Mapping joinMapping) {
        return new Mapping(joinMapping.name, this);
    }

    /**
     * Returns the field name for which this mapping was created.
     * <p>
     * Note that this is not necessarily the property name as for properties in mixins or compounds, the
     * parent fields are appended separated by {@link #SUBFIELD_SEPARATOR}.
     *
     * @return the field name for which this mapping was created
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the parent field.
     *
     * @return the parent field (the composite or mixin which contains this mapping). Returns <tt>null</tt> if this is
     * a top-level field of an entity
     */
    @Nullable
    public Mapping getParent() {
        return parent;
    }

    @Override
    public String toString() {
        if (parent != null) {
            return parent + "." + name;
        } else {
            return name;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (obj.getClass() != Mapping.class) {
            return false;
        }

        Mapping other = (Mapping) obj;

        return Strings.areEqual(name, other.getName()) && Objects.equals(parent, other.getParent());
    }

    @Override
    public int hashCode() {
        if (parent == null) {
            return name.hashCode();
        }

        return name.hashCode() + 17 * parent.hashCode();
    }
}
