/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import com.google.common.collect.Maps;
import sirius.db.mixing.Entity;
import sirius.db.mixing.EntityRef;
import sirius.kernel.Sirius;
import sirius.kernel.commons.Amount;
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
import java.sql.Time;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
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
 */
@Register(classes = Databases.class)
public class Databases {

    protected static final Log LOG = Log.get("db");
    protected static final Log SLOW_DB_LOG = Log.get("db-slow");
    private static final Map<String, Database> datasources = Maps.newConcurrentMap();

    @ConfigValue("jdbc.logQueryThreshold")
    private static Duration longQueryThreshold;
    private static long longQueryThresholdMillis = -1;

    @ConfigValue("jdbc.logConnectionThreshold")
    private static Duration longConnectionThreshold;
    private static long longConnectionThresholdMillis = -1;

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
                collector.differentialMetric("jdbc-use", "db-uses", "JDBC Uses", numUses.getCount(), "/min");
                collector.differentialMetric("jdbc-connects",
                                             "db-connects",
                                             "JDBC Connects",
                                             numConnects.getCount(),
                                             "/min");

                int highestUtilization = determineHighestUtilization();
                collector.metric("db-pool-utilization", "JDBC Pool Utilization (max)", highestUtilization, "%");

                collector.differentialMetric("jdbc-queries",
                                             "db-queries",
                                             "JDBC Queries",
                                             numQueries.getCount(),
                                             "/min");
                collector.differentialMetric("jdbc-slow-queries",
                                             "db-slow-queries",
                                             "Slow JDBC Queries",
                                             numSlowQueries.getCount(),
                                             "/min");
                collector.metric("db-query-duration", "JDBC Query Duration", queryDuration.getAndClearAverage(), "ms");
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
        return Sirius.getSettings()
                     .getExtensions("jdbc.database")
                     .stream()
                     .map(Extension::getId)
                     .collect(Collectors.toList());
    }

    /**
     * Determines if a databse with the given name is present in the configuration.
     *
     * @param name the name of the database
     * @return <tt>true</tt> if a configuration <tt>jdbc.database.[name]</tt> does exist, <tt>false</tt> otherwise
     */
    public boolean hasDatabase(String name) {
        Extension extension = Sirius.getSettings().getExtension("jdbc.database", name);
        return extension != null && !extension.isDefault();
    }

    /**
     * Converts the threshold for "slow queries" into a long containing milliseconds for performance reasons.
     *
     * @return the threshold for long queries in milliseconds
     */
    protected static long getLongQueryThresholdMillis() {
        if (longQueryThresholdMillis < 0) {
            longQueryThresholdMillis = longQueryThreshold.toMillis();
        }

        return longQueryThresholdMillis;
    }


    /**
     * Converts the threshold for "long connections" into a long containing milliseconds for performance reasons.
     *
     * @return the threshold for long queries in milliseconds
     */
    protected static long getLongConnectionThresholdMillis() {
        if (longConnectionThresholdMillis < 0) {
            longConnectionThresholdMillis = longConnectionThreshold.toMillis();
        }

        return longConnectionThresholdMillis;
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
        if (value instanceof LocalDateTime) {
            return encodeLocalDateTime((LocalDateTime) value);
        }
        if (value instanceof LocalDate) {
            return Date.valueOf((LocalDate) value);
        }
        if (value instanceof LocalTime) {
            return Time.valueOf((LocalTime) value);
        }
        if (value instanceof Amount) {
            return ((Amount) value).getAmount();
        }
        if (value.getClass().isEnum()) {
            return ((Enum<?>) value).name();
        }
        if (value instanceof EntityRef) {
            return ((EntityRef<?>) value).getId();
        }
        if (value instanceof Entity) {
            return ((Entity) value).getId();
        }

        return value;
    }
}
