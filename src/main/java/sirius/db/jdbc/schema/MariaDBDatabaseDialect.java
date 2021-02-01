/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.schema;

import sirius.kernel.di.std.Register;

/**
 * Defines the dialect used to sync the schema against a MariaDB database.
 * <p>
 * This is for now equal to a MySQL database.
 */
@Register(name = "mariadb", classes = DatabaseDialect.class)
public class MariaDBDatabaseDialect extends MySQLDatabaseDialect {

}
