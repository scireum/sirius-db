/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.schema;

/**
 * Provides the schema information for a {@link sirius.db.mixing.Property} required to build a database schema in a
 * SQL / JDBC database.
 */
public interface SQLPropertyInfo {

    /**
     * Appends columns, keys and foreign keys to the given table to match the settings specified by
     * this property
     *
     * @param table the table to add schema infos to
     */
    void contributeToTable(Table table);
}
