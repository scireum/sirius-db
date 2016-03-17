/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import com.google.common.collect.Maps;
import sirius.kernel.di.std.Register;
import sirius.kernel.extensions.Extensions;
import sirius.kernel.health.Average;
import sirius.kernel.health.Counter;
import sirius.kernel.health.Log;
import sirius.kernel.health.metrics.MetricProvider;
import sirius.kernel.health.metrics.MetricsCollector;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Provides a {@link javax.sql.DataSource} which can be configured via the system
 * configuration.
 * <p>
 * Use {@link #get(String)} to obtain a managed connection to the given database.
 * <p>
 * Configuration is done via the system configuration. To declare a database provide an extension in
 * <tt>jdbc.database</tt>. For examples see "component-db.conf".
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2013/11
 */
@Register(classes = Databases.class)
public class Databases {

    protected static final Log LOG = Log.get("db");
    private static final Map<String, Database> datasources = Maps.newConcurrentMap();

    protected static Counter numUses = new Counter();
    protected static Counter numQueries = new Counter();
    protected static Average queryDuration = new Average();

    /**
     * Provides some metrics across all managed data sources.
     */
    @Register
    public static class DatabaseMetricProvider implements MetricProvider {

        @Override
        public void gather(MetricsCollector collector) {
            // Only report statistics if we have at least one database connection...
            if (!datasources.isEmpty()) {
                collector.differentialMetric("jdbc-use", "db-uses", "JDBC Uses", numUses.getCount(), null);
                collector.differentialMetric("jdbc-queries", "db-queries", "JDBC Queries", numQueries.getCount(), null);
                collector.metric("db-query-duration", "JDBC Query Duration", queryDuration.getAndClearAverage(), "ms");
            }
        }
    }

    /**
     * Provides access to the selected database.
     * <p>
     * The configuration of the connection pool will be loaded from <tt>jdbc.database.[name]</tt>
     *
     * @param name name of the database to access
     * @return a wrapper providing access to the given database
     */
    public Database get(String name) {
        Database ds = datasources.get(name);
        if (ds == null) {
            synchronized (datasources) {
                ds = datasources.get(name);
                if (ds == null) {
                    ds = new Database(name);
                    datasources.put(name, ds);
                }
            }
        }
        return ds;
    }

    /**
     * Returns a list of all known (configured) databases from the system config.
     *
     * @return a list of all known databases
     */
    public List<String> getDatabases() {
        return Extensions.getExtensions("jdbc.database").stream().map(e -> e.getId()).collect(Collectors.toList());
    }

    /**
     * Determines if a databse with the given name is present in the configuration.
     *
     * @param name the name of the database
     * @return <tt>true</tt> if a configuration <tt>jdbc.database.[name]</tt> does exist, <tt>false</tt> otherwise
     */
    public boolean hasDatabase(String name) {
        return Extensions.getExtension("jdbc.database", name) != null;
    }
}
