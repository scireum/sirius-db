/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.jdbc.SQLEntity;

/**
 * Represents an entity which reports an empty string representation.
 * <p>
 * Overriding <tt>toString()</tt> is common for entities which render a label built from optional fields. Such a label
 * may well be empty, which must not influence any comparison based on the id of the entity.
 */
public class EmptyToStringEntity extends SQLEntity {

    @Override
    public String toString() {
        return "";
    }
}
