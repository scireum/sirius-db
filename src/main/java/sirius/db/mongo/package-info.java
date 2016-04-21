/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

/**
 * Thin layer above Mongo DB.
 * <p>
 * Basically this provides a connection pool which can be configured via the system configuration (namely
 * <tt>mongo.host</tt> and <tt>mongo.db</tt>).
 * <p>
 * It also provides fluent query builders for CRUD operations.
 *
 * @see sirius.db.mongo.Mongo
 */
package sirius.db.mongo;