/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.schema;

import sirius.kernel.di.std.Register;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;

/**
 * Defines the dialect used to sync the schema against anMariaDB database.
 *
 * This is for now equal to a MySQL database.
 */
@Register(name = "mariadb", classes = DatabaseDialect.class)
public class MariaDBDatabaseDialect extends MySQLDatabaseDialect {

}
