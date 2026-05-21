/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.kernel.Sirius;
import sirius.kernel.async.Operation;
import sirius.kernel.commons.Context;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;
import sirius.kernel.di.std.Part;
import sirius.kernel.health.Exceptions;
import sirius.kernel.nls.Formatter;
import sirius.kernel.settings.Extension;
import sirius.kernel.settings.PortMapper;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a database connection obtained via {@link Databases#get(String)}.
 * <p>
 * Use {@link #createQuery(String)} to create an SQL query with built-in connection management.
 * Use {@link #getConnection()} to obtain a regular JDBC connection (which has to be handled with some caution).
 */
public class Database {

    @Part
    private static Databases dbs;

    private static final String KEY_DRIVER = "driver";
    private static final String KEY_URL = "url";
    private static final String KEY_HOST_URL = "hostUrl";
    private static final String KEY_SERVICE = "service";
    private static final String KEY_USER = "user";
    private static final String KEY_PASSWORD = "password";
    private static final String KEY_INITIAL_SIZE = "initialSize";
    private static final String KEY_MAX_ACTIVE = "maxActive";
    private static final String KEY_MAX_IDLE = "maxIdle";
    private static final String KEY_VALIDATION_QUERY = "validationQuery";
    protected final String name;
    private final String service;
    private final String driver;
    private String url;
    private String hostUrl;
    private final String username;
    private final String password;
    private final int initialSize;
    private final int maxActive;
    private final int maxIdle;
    private final boolean testOnBorrow;
    private final String validationQuery;
    private MonitoredDataSource dataSource;
    private Set<Capability> capabilities;
    private static final Pattern SANE_COLUMN_NAME = Pattern.compile("\\w+");
    private static final Pattern HOST_AND_PORT_PATTERN = Pattern.compile("//([^:]+):(\\d+)");

    /*
     * Use the get(name) method to create a new object.
     */
    protected Database(String name) {
        Extension extension = Sirius.getSettings().getExtension("jdbc.database", name);
        if (extension == null) {
            throw Exceptions.handle()
                            .to(Databases.LOG)
                            .withSystemErrorMessage("Unknown JDBC database: %s", name)
                            .handle();
        }
        Extension profile =
                Sirius.getSettings().getExtension("jdbc.profile", extension.get("profile").asString("default"));
        Context context = profile.getContext();
        context.putAll(extension.getContext());
        this.name = name;
        this.driver = extension.get(KEY_DRIVER).isEmptyString() ?
                      Formatter.create(profile.get(KEY_DRIVER).asString()).setDirect(context).format() :
                      extension.get(KEY_DRIVER).asString();
        this.service = extension.get(KEY_SERVICE).isEmptyString() ?
                       Formatter.create(profile.get(KEY_SERVICE).asString()).setDirect(context).format() :
                       extension.get(KEY_SERVICE).asString();
        this.url = extension.get(KEY_URL).isEmptyString() ?
                   Formatter.create(profile.get(KEY_URL).asString()).setDirect(context).format() :
                   extension.get(KEY_URL).asString();
        this.hostUrl = extension.get(KEY_HOST_URL).isEmptyString() ?
                       Formatter.create(profile.get(KEY_HOST_URL).asString()).setDirect(context).format() :
                       extension.get(KEY_HOST_URL).asString();
        applyPortMapping();
        this.username = extension.get(KEY_USER).isEmptyString() ?
                        Formatter.create(profile.get(KEY_USER).asString()).setDirect(context).format() :
                        extension.get(KEY_USER).asString();
        this.password = extension.get(KEY_PASSWORD).isEmptyString() ?
                        Formatter.create(profile.get(KEY_PASSWORD).asString()).setDirect(context).format() :
                        extension.get(KEY_PASSWORD).asString();
        this.initialSize = extension.get(KEY_INITIAL_SIZE).isFilled() ?
                           extension.get(KEY_INITIAL_SIZE).asInt(0) :
                           profile.get(KEY_INITIAL_SIZE).asInt(0);
        this.maxActive = extension.get(KEY_MAX_ACTIVE).isFilled() ?
                         extension.get(KEY_MAX_ACTIVE).asInt(10) :
                         profile.get(KEY_MAX_ACTIVE).asInt(10);
        this.maxIdle = extension.get(KEY_MAX_IDLE).isFilled() ?
                       extension.get(KEY_MAX_IDLE).asInt(1) :
                       profile.get(KEY_MAX_IDLE).asInt(1);
        this.validationQuery = extension.get(KEY_VALIDATION_QUERY).isEmptyString() ?
                               Formatter.create(profile.get(KEY_VALIDATION_QUERY).asString())
                                        .setDirect(context)
                                        .format() :
                               extension.get(KEY_VALIDATION_QUERY).asString();
        this.testOnBorrow = Strings.isFilled(validationQuery);
    }

    private void applyPortMapping() {
        Matcher hostAndPortMatcher = HOST_AND_PORT_PATTERN.matcher(this.url);
        if (hostAndPortMatcher.find()) {
            Tuple<String, Integer> effectiveHostAndPort = PortMapper.mapPort(this.service,
                                                                             hostAndPortMatcher.group(1),
                                                                             Integer.parseInt(hostAndPortMatcher.group(2)));
            this.url = hostAndPortMatcher.replaceFirst("//"
                                                       + effectiveHostAndPort.getFirst()
                                                       + ":"
                                                       + effectiveHostAndPort.getSecond());
        }

        hostAndPortMatcher = HOST_AND_PORT_PATTERN.matcher(this.hostUrl);
        if (hostAndPortMatcher.find()) {
            Tuple<String, Integer> effectiveHostAndPort = PortMapper.mapPort(this.service,
                                                                             hostAndPortMatcher.group(1),
                                                                             Integer.parseInt(hostAndPortMatcher.group(2)));
            this.hostUrl = hostAndPortMatcher.replaceFirst("//"
                                                           + effectiveHostAndPort.getFirst()
                                                           + ":"
                                                           + effectiveHostAndPort.getSecond());
        }
    }

    /**
     * Provides access to the underlying {@link DataSource} representing the connection pool.
     * <p>
     * You must ensure to close each opened connection property as otherwise the pool will lock up, once all
     * connections are busy. Consider using {@link #createQuery(String)} or
     * {@link #createFunctionCall(String, Integer)} or {@link #createProcedureCall(String)} to access the database
     * in a safe manner.
     *
     * @return the connection pool as DataSource
     */
    public DataSource getDatasource() {
        if (dataSource == null) {
            dataSource = new MonitoredDataSource();
            initialize();
        }
        return dataSource;
    }

    /**
     * Creates a new connection to the database.
     * <p>
     * You must ensure to close each opened connection property as otherwise the pool will lock up, once all
     * connections are busy. Consider using {@link #createQuery(String)} or
     * {@link #createFunctionCall(String, Integer)} or {@link #createProcedureCall(String)} to access the database
     * in a safe manner.
     *
     * @return a new {@link Connection} to the database
     * @throws SQLException in case of a database error
     */
    public Connection getConnection() throws SQLException {
        try (var _ = createOperation("getConnection()")) {
            return new WrappedConnection(getDatasource().getConnection(), this);
        }
    }

    private Operation createOperation(String methodName) {
        return new Operation(() -> "Database: " + name + "." + methodName, Duration.ofSeconds(5));
    }

    /**
     * Creates a new connection to the database.
     * <p>
     * This connection behaves entirely the same as one returned by {@link #getConnection()}. However, for this
     * connection, the checks are disabled which ensure that connections aren't borrowed for too long. Although,
     * this check is very helpful under normal conditions, it generates false warning when performing long-running
     * {@link sirius.db.jdbc.batch.BatchContext batch operations} - which provide their own way of monitoring.
     *
     * @return a new {@link Connection} to the database
     * @throws SQLException in case of a database error
     */
    @SuppressWarnings("squid:S2095")
    @Explain("We return this method - therefore properly calling close is the responsibility of the caller.")
    public Connection getLongRunningConnection() throws SQLException {
        try (var _ = createOperation("getLongRunningConnection()")) {
            return new WrappedConnection(getDatasource().getConnection(), this).markAsLongRunning();
        }
    }

    /**
     * Tries to obtain a host connection which is not bound to a specific database or schema.
     * <p>
     * This is used to set up test databases by executing an initial statement like <tt>CREATE DATABASE test</tt>.
     *
     * @return the host connection
     * @throws SQLException in case of a database or configuration error
     */
    public Connection getHostConnection() throws SQLException {
        if (Strings.isEmpty(hostUrl)) {
            return getConnection();
        }

        try (var _ = createOperation("getHostConnection()")) {
            try {
                Class.forName(driver);
            } catch (ClassNotFoundException exception) {
                Exceptions.handle(OMA.LOG, exception);
            }

            return DriverManager.getConnection(hostUrl, username, password);
        }
    }

    /**
     * Creates a new query wrapper which permits safe and convenient queries.
     * <p>
     * Using this wrapper ensures proper connection handling and simplifies query creation.
     *
     * @param sql the SQL to send to the database. For syntax help concerning parameter names and optional query parts,
     *            see {@link SQLQuery}
     * @return a new query which can be supplied with parameters and executed against the database
     * @see SQLQuery
     */
    public SQLQuery createQuery(String sql) {
        return new SQLQuery(this, sql);
    }

    /**
     * Creates a new call wrapper which permits safe and convenient function calls.
     *
     * @param functionName name of the function to call
     * @param returnType   the SQL type ({@link Types}) of the return value of this function
     * @return a new call which can be supplied with parameters and executed against the database
     */
    public SQLCall createFunctionCall(String functionName, Integer returnType) {
        return new SQLCall(this, functionName, returnType);
    }

    /**
     * Creates a new call wrapper which permits safe and convenient procedure calls.
     *
     * @param procedureName name of the procedure to call
     * @return a new call which can be supplied with parameters and executed against the database
     */
    public SQLCall createProcedureCall(String procedureName) {
        return new SQLCall(this, procedureName, null);
    }

    /**
     * Generates an INSERT statement for the given table inserting all supplied parameters in <tt>ctx</tt>.
     *
     * @param table   the target table to insert a row
     * @param context contains names and values to insert into the database
     * @return a Row containing all generated keys
     * @throws SQLException in case of a database error
     */
    @SuppressWarnings("squid:S2077")
    @Explain("prepareValues verifies the field names and converts all values into parameters for the prepared statement")
    public Row insertRow(String table, Context context) throws SQLException {
        try (Connection connection = getConnection()) {
            StringBuilder fields = new StringBuilder();
            StringBuilder values = new StringBuilder();
            List<Object> valueList = new ArrayList<>();
            prepareValues(context, fields, values, valueList);
            String sql = "INSERT INTO " + table + " (" + fields + ") VALUES(" + values + ")";
            try (PreparedStatement statement = hasCapability(Capability.GENERATED_KEYS) ?
                                               connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS) :
                                               connection.prepareStatement(sql)) {
                fillValues(valueList, sql, statement);
                statement.executeUpdate();
                return dbs.fetchGeneratedKeys(statement);
            }
        }
    }

    protected void fillValues(List<Object> values, String sql, PreparedStatement statement) {
        int index = 0;
        for (Object value : values) {
            try {
                Databases.convertAndSetParameter(statement, ++index, value);
            } catch (Exception exception) {
                throw new IllegalArgumentException(exception.getMessage()
                                                   + " - Index: "
                                                   + index
                                                   + ", Value: "
                                                   + value
                                                   + ", Query: "
                                                   + sql, exception);
            }
        }
    }

    protected void prepareValues(Context context, StringBuilder fields, StringBuilder values, List<Object> valueList) {
        for (Map.Entry<String, Object> entry : context.entrySet()) {
            if (entry.getValue() != null) {
                if (!fields.isEmpty()) {
                    fields.append(", ");
                    values.append(", ");
                }
                if (!SANE_COLUMN_NAME.matcher(entry.getKey()).matches()) {
                    throw Exceptions.handle()
                                    .to(Databases.LOG)
                                    .withSystemErrorMessage("Cannot use '%s' as column name for an insert. "
                                                            + "Only characters, digits and '_' is allowed!",
                                                            entry.getKey())
                                    .handle();
                }
                fields.append(entry.getKey());
                values.append("?");
                valueList.add(entry.getValue());
            }
        }
    }

    /**
     * Boilerplate method to use {@link #insertRow(String, sirius.kernel.commons.Context)} with plan maps.
     *
     * @param table the target table to insert a row
     * @param row   contains names and values to insert into the database
     * @return a Row containing all generated keys
     * @throws SQLException in case of a database error
     */
    public Row insertRow(String table, Map<String, Object> row) throws SQLException {
        Context context = Context.create();
        context.putAll(row);
        return insertRow(table, context);
    }

    private void initialize() {
        if (dataSource != null) {
            dataSource.setDriverClassName(driver);
            dataSource.setUrl(url);
            dataSource.setUsername(username);
            dataSource.setPassword(password);
            dataSource.setInitialSize(initialSize);
            dataSource.setMaxTotal(maxActive == 0 ? 20 : maxActive);
            dataSource.setMaxIdle(maxIdle);
            dataSource.setTestOnBorrow(testOnBorrow);
            dataSource.setValidationQuery(validationQuery);
            dataSource.setMaxWait(Duration.ofSeconds(1));
        }
    }

    /**
     * Returns the JDBC connection URL
     *
     * @return the JDBC connection URL used to connect to the database
     */
    public String getUrl() {
        return url;
    }

    /**
     * Returns the JDBC username supplied to the database.
     *
     * @return the username supplied to the database
     */
    public String getUsername() {
        return username;
    }

    /**
     * Returns the JDBC password supplied to the database.
     *
     * @return the password supplied to the database
     */
    public String getPassword() {
        return password;
    }

    /**
     * Returns the maximal number of concurrent connections
     *
     * @return the maximal number of concurrent connections
     */
    public int getSize() {
        return maxActive;
    }

    /**
     * Return the number of idle connections
     *
     * @return the number of open but unused connection
     */
    public int getNumIdle() {
        if (dataSource == null) {
            return 0;
        }
        return dataSource.getNumIdle();
    }

    /**
     * Return the number of active connections
     *
     * @return the number of open and active connection
     */
    public int getNumActive() {
        if (dataSource == null) {
            return 0;
        }
        return dataSource.getNumActive();
    }

    /**
     * Determines if the current driver has the requested capability.
     *
     * @param cap the capability to determine
     * @return <tt>true</tt> if the capability is supported / present, <tt>false</tt> otherwise
     */
    public boolean hasCapability(Capability cap) {
        if (capabilities == null) {
            if ("com.mysql.jdbc.Driver".equalsIgnoreCase(driver)
                || "org.mariadb.jdbc.Driver".equalsIgnoreCase(driver)) {
                capabilities = Capability.MYSQL_CAPABILITIES;
            } else if ("org.postgresql.Driver".equalsIgnoreCase(driver)) {
                capabilities = Capability.POSTGRES_CAPABILITIES;
            } else if ("com.clickhouse.jdbc.Driver".equals(driver)) {
                capabilities = Capability.CLICKHOUSE_CAPABILITIES;
            } else {
                capabilities = Capability.DEFAULT_CAPABILITIES;
            }
        }

        return capabilities.contains(cap);
    }

    @Override
    public String toString() {
        return Strings.apply("%s (%d/%d)", name, getNumActive(), getSize());
    }
}
