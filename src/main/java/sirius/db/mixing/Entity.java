/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

/**
 * Describes methods implemented in {@link BaseEntity} and therefore available for all entities.
 * <p>
 * The reason to define an extra interface is to have a base interface when defining database independent entities.
 * These are commonly defined as interface and then implemented by one or more subclasses of specific entities.
 * However e.g. the templates often refert to the interface itself so there is only on UI to manage all kinds of
 * entities. Therefore we need a super interface so that tagliatelle (which only sees the interface) knows which
 * methods are available.@
 */
public interface Entity {

    /**
     * Determines if the entity is new (not yet written to the database).
     *
     * @return <tt>true</tt> if the entity has not been written to the database yes, <tt>false</tt> otherwise
     */
    boolean isNew();

    /**
     * Each entity type can be addressed by its class or by a unique name, which is its simple class name in upper
     * case.
     *
     * @return the type name of this entity type
     * @see #getUniqueName()
     */
    String getTypeName();

    /**
     * Returns an unique name of this entity.
     * <p>
     * This unique string representation of this entity is made up of its type along with its id.
     *
     * @return an unique representation of this entity or an empty string if the entity was not written to the
     * database yet
     */
    String getUniqueName();

    /**
     * Returns a string representation of the entity ID.
     * <p>
     * If the entity is new, "new" will be returned.
     *
     * @return the entity ID as string or "new" if the entity {@link #isNew()}.
     */
    String getIdAsString();

    /**
     * Provides a simple re-definition so that this method is visible to Tagliatelle as well.
     *
     * @return a string representation of this entity
     */
    @Override
    String toString();
}
