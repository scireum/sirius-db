/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.types.BaseEntityRef;
import sirius.kernel.Sirius;
import sirius.kernel.commons.Amount;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.di.Initializable;
import sirius.kernel.di.std.ConfigValue;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Average;
import sirius.kernel.health.Counter;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Log;
import sirius.kernel.health.metrics.MetricProvider;
import sirius.kernel.health.metrics.MetricsCollector;
import sirius.kernel.settings.Extension;

import javax.annotation.Nullable;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides a {@link javax.sql.DataSource} which can be configured via the system
 * configuration.
 * <p>
 * Use {@link #get(String)} to obtain a managed connection to the given database.
 * <p>
 * Configuration is done via the system configuration. To declare a database provide an extension in
 * <tt>jdbc.database</tt>. For examples see "component-db.conf".
 */
@Register(classes = {Databases.class, Initializable.class})
public class Databases implements Initializable {

    protected static final Log LOG = Log.get("jdbc");
    private static final Map<String, Database> datasources = new ConcurrentHashMap<>();

    @ConfigValue("jdbc.logQueryThreshold")
    private static Duration logQueryThreshold;
    private static long logQueryThresholdMillis = -1;

    @ConfigValue("jdbc.logConnectionThreshold")
    private static Duration logConnectionThreshold;
    private static long logConnectionThresholdMillis = -1;

    protected static Counter numUses = new Counter();
    protected static Counter numConnects = new Counter();
    protected static Counter numQueries = new Counter();
    protected static Counter numSlowQueries = new Counter();
    protected static Average queryDuration = new Average();

    private static final long SECOND_SHIFT = 1;
    private static final long MINUTE_SHIFT = SECOND_SHIFT * 100;
    private static final long HOUR_SHIFT = MINUTE_SHIFT * 100;
    private static final long DAY_SHIFT = HOUR_SHIFT * 100;
    private static final long MONTH_SHIFT = DAY_SHIFT * 100;
    private static final long YEAR_SHIFT = MONTH_SHIFT * 100;

    /**
     * Provides some metrics across all managed data sources.
     */
    @Register
    public static class DatabaseMetricProvider implements MetricProvider {

        @Override
        public void gather(MetricsCollector collector) {
            // Only report statistics if we have at least one database connection...
            if (!datasources.isEmpty()) {
                collector.differentialMetric("jdbc_use", "db-uses", "JDBC Uses", numUses.getCount(), "/min");
                collector.differentialMetric("jdbc_connects",
                                             "db-connects",
                                             "JDBC Connects",
                                             numConnects.getCount(),
                                             "/min");

                int highestUtilization = determineHighestUtilization();
                collector.metric("jdbc_pool_utilization",
                                 "db-pool-utilization",
                                 "JDBC Pool Utilization (max)",
                                 highestUtilization,
                                 "%");

                collector.differentialMetric("jdbc_queries",
                                             "db-queries",
                                             "JDBC Queries",
                                             numQueries.getCount(),
                                             "/min");
                collector.differentialMetric("jdbc_slow_queries",
                                             "db-slow-queries",
                                             "Slow JDBC Queries",
                                             numSlowQueries.getCount(),
                                             "/min");
                collector.metric("jdbc_query_duration",
                                 "db-query-duration",
                                 "JDBC Query Duration",
                                 queryDuration.getAndClear(),
                                 "ms");
            }
        }

        protected int determineHighestUtilization() {
            int highestUtilization = 0;
            for (Database db : datasources.values()) {
                highestUtilization = Math.max(highestUtilization, db.getNumActive() * 100 / db.getSize());
            }
            return highestUtilization;
        }
    }

    @Override
    public void initialize() throws Exception {
        datasources.clear();
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
                ds = datasources.computeIfAbsent(name, k -> new Database(name));
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
        return Sirius.getSettings().getExtensions("jdbc.database").stream().map(Extension::getId).toList();
    }

    /**
     * Determines if a database with the given name is present in the configuration.
     *
     * @param name the name of the database
     * @return <tt>true</tt> if a configuration <tt>jdbc.database.[name]</tt> does exist, <tt>false</tt> otherwise
     */
    public boolean hasDatabase(@Nullable String name) {
        if (Strings.isEmpty(name)) {
            return false;
        }

        Extension extension = Sirius.getSettings().getExtension("jdbc.database", name);
        return extension != null && !extension.isDefault();
    }

    /**
     * Converts the threshold for "slow queries" into a long containing milliseconds for performance reasons.
     *
     * @return the threshold for long queries in milliseconds
     */
    protected static long getLogQueryThresholdMillis() {
        if (logQueryThresholdMillis < 0) {
            logQueryThresholdMillis = logQueryThreshold.toMillis();
        }

        return logQueryThresholdMillis;
    }

    /**
     * Converts the threshold for "long connections" into a long containing milliseconds for performance reasons.
     *
     * @return the threshold for long queries in milliseconds
     */
    protected static long getLogConnectionThresholdMillis() {
        if (logConnectionThresholdMillis < 0) {
            logConnectionThresholdMillis = logConnectionThreshold.toMillis();
        }

        return logConnectionThresholdMillis;
    }

    /**
     * Encodes a <tt>LocalDateTime</tt> as a long.
     * <p>
     * Basically the generated long consists of year_month_day_hour_minute_second to support sorting and ordering.
     * Also we do not use timestamps as MySQL does autoupdate these unexpectedly and also because we do not need the
     * millisecond resolution (it can even lead to errors).
     *
     * @param date the date to encode
     * @return the date encoded as a sortable long or -1 if the given value was <tt>null</tt>
     */
    public static long encodeLocalDateTime(@Nullable LocalDateTime date) {
        if (date == null) {
            return -1;
        }
        return date.getSecond() * SECOND_SHIFT
               + date.getMinute() * MINUTE_SHIFT
               + date.getHour() * HOUR_SHIFT
               + date.getDayOfMonth() * DAY_SHIFT
               + date.getMonthValue() * MONTH_SHIFT
               + date.getYear() * YEAR_SHIFT;
    }

    /**
     * Decodes a long back into a <tt>LocalDateTime</tt>.
     * <p>
     * This is the inverse of {@link #encodeLocalDateTime(LocalDateTime)}.
     *
     * @param timestamp the number to decode
     * @return the decoded date and time or <tt>null</tt> if the given number was negative
     */
    @Nullable
    public static LocalDateTime decodeLocalDateTime(long timestamp) {
        if (timestamp < 0) {
            return null;
        }
        long date = timestamp;
        int year = (int) (date / YEAR_SHIFT);
        date = date % YEAR_SHIFT;

        int month = (int) (date / MONTH_SHIFT);
        date = date % MONTH_SHIFT;

        int day = (int) (date / DAY_SHIFT);
        date = date % DAY_SHIFT;

        int hour = (int) (date / HOUR_SHIFT);
        date = date % HOUR_SHIFT;

        int minute = (int) (date / MINUTE_SHIFT);
        date = date % MINUTE_SHIFT;

        int second = (int) (date / SECOND_SHIFT);

        try {
            return LocalDateTime.of(year, month, day, hour, minute, second);
        } catch (DateTimeException e) {
            Exceptions.ignore(e);
            return Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime();
        }
    }

    /**
     * Transforms the given value into its database representation.
     *
     * @param value the value to transform
     * @return the database level representation of the given value
     */
    public static Object convertValue(Object value) {
        if (value == null) {
            return value;
        }
        if (value instanceof LocalDateTime dateTime) {
            return encodeLocalDateTime(dateTime);
        }
        if (value instanceof LocalDate date) {
            return Date.valueOf(date);
        }
        if (value instanceof LocalTime time) {
            return Time.valueOf(time);
        }
        if (value instanceof Amount amount) {
            return amount.getAmount();
        }
        if (value.getClass().isEnum()) {
            return ((Enum<?>) value).name();
        }
        if (value instanceof BaseEntityRef) {
            return ((BaseEntityRef<?, ?>) value).getId();
        }
        if (value instanceof BaseEntity) {
            return ((BaseEntity<?>) value).getId();
        }

        return value;
    }

    /**
     * Reads and returns all available columns of the given result set as upper case.
     *
     * @param rs the result set to parse
     * @return a set of columns within the given result set
     * @throws SQLException in case of a database error
     */
    public Set<String> readColumns(ResultSet rs) throws SQLException {
        Set<String> result = new HashSet<>();
        for (int col = 1; col <= rs.getMetaData().getColumnCount(); col++) {
            result.add(rs.getMetaData().getColumnLabel(col).toUpperCase());
        }

        return result;
    }

    /**
     * Returns all generated keys wrapped as row
     *
     * @param stmt the statement which was used to perform an update or insert
     * @return a row containing all generated keys
     * @throws SQLException in case of an error thrown by the database or driver
     */
    public Row fetchGeneratedKeys(PreparedStatement stmt) throws SQLException {
        try (ResultSet rs = stmt.getGeneratedKeys()) {
            Row row = new Row();
            if (rs != null && rs.next()) {
                for (int col = 1; col <= rs.getMetaData().getColumnCount(); col++) {
                    row.fields.put(rs.getMetaData().getColumnLabel(col).toUpperCase(),
                                   Tuple.create(rs.getMetaData().getColumnLabel(col), rs.getObject(col)));
                }
            }
            return row;
        }
    }
}
