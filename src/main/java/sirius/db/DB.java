/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db;

import sirius.kernel.health.Log;

/**
 * Provides constants used throughout <tt>sirius-db</tt>.
 */
public class DB {

    /**
     * Contains a generic logger to be used when a slow database query is detected.
     */
    public static final Log SLOW_DB_LOG = Log.get("db-slow");

    /**
     * Constructor is locked as this class only has static members.
     */
    private DB() {
    }
}
