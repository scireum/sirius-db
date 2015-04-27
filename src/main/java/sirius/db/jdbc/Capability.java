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
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2015/04
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
    LIMIT;

    /**
     * Contains the capabilities of a MySQL database
     */
    public static final EnumSet<Capability> MYSQL_CAPABILITIES = EnumSet.of(STREAMING, LIMIT);

    /**
     * Contains the capabilities of a HSQL database
     */
    public static final EnumSet<Capability> HSQLDB_CAPABILITIES = EnumSet.of(LIMIT);

    /**
     * Contains the capabilities of a Postgres database
     */
    public static final EnumSet<Capability> POSTGRES_CAPABILITIES = EnumSet.of(LIMIT);
}
