/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import java.util.EnumSet;

/**
 * Encapsulates functions or features which are not supported by all databases.
 * <p>
 * The capabilities of the connected db can be determined by calling {@link Database#hasCapability(Capability)}.
 * The capabilities are determined by checking the driver name.
 */
public enum Capability {
    /**
     * Signals that a streaming result is only created if the FETCH_SIZE is set to Integer.MIN_VALUE. This
     * is more or less a MySQL specific behaviour as other drivers honor the "FETCH_SIZE" hint which MySQL
     * doesn't.
     */
    STREAMING,

    /**
     * Determines if the connected database support the LIMIT clause in SQL queries
     */
    LIMIT,

    /**
     * Determines if the database potentially lowercases table names like MySQL commonly does. In this
     * case the best bet is to only use lower cased table names in the first place to avoid a lot of
     * trouble.
     */
    LOWER_CASE_TABLE_NAMES;

    /**
     * Contains the capabilities of a MySQL database
     */
    public static final EnumSet<Capability> MYSQL_CAPABILITIES = EnumSet.of(LOWER_CASE_TABLE_NAMES, STREAMING, LIMIT);

    /**
     * Contains the capabilities of a HSQL database
     */
    public static final EnumSet<Capability> HSQLDB_CAPABILITIES = EnumSet.of(LIMIT);

    /**
     * Contains the capabilities of a Postgres database
     */
    public static final EnumSet<Capability> POSTGRES_CAPABILITIES = EnumSet.of(LIMIT);
}
