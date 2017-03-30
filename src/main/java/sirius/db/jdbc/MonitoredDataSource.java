/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.ConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Tracks how many connections are actually created.
 * <p>
 * Even if connections are short-lived and not concurrently created, they could still drain the pool of local TCP ports
 * of the OS. Therefore we track the number of total created connections and warn if there are too many - this is
 * a strong indication that the connection pool is misconfigured and not working as expected anyway.
 */
class MonitoredDataSource extends BasicDataSource {

    @Override
    protected ConnectionFactory createConnectionFactory() throws SQLException {
        ConnectionFactory actualFactory = super.createConnectionFactory();

        return new ConnectionFactory() {
            @Override
            public Connection createConnection() throws SQLException {
                Databases.numConnects.inc();
                return actualFactory.createConnection();
            }
        };
    }
}
