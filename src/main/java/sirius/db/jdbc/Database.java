/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import com.google.common.collect.Lists;
import sirius.kernel.async.Operation;
import sirius.kernel.commons.Context;
import sirius.kernel.commons.Strings;
import sirius.kernel.extensions.Extension;
import sirius.kernel.extensions.Extensions;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;
import sirius.kernel.nls.Formatter;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents a database connection obtained via {@link Databases#get(String)}.
 * <p>
 * Use {@link #createQuery(String)} to create an SQL query with built in connection management.
 * Use {@link #getConnection()} to obtain a regular JDBC connection (which has to be handled with some caution).
 */
public class Database {

    private static final String KEY_DRIVER = "driver";
    private static final String KEY_URL = "url";
    private static final String KEY_USER = "user";
    private static final String KEY_PASSWORD = "password";
    private static final String KEY_INITIAL_SIZE = "initialSize";
    private static final String KEY_MAX_ACTIVE = "maxActive";
    private static final String KEY_MAX_IDLE = "maxIdle";
    private static final String KEY_VALIDATION_QUERY = "validationQuery";
    protected final String name;
    private String driver;
    private String url;
    private String username;
    private String password;
    private int initialSize;
    private int maxActive;
    private int maxIdle;
    private boolean testOnBorrow;
    private String validationQuery;
    private MonitoredDataSource ds;
    private Set<Capability> capabilities;

    /*
     * Use the get(name) method to create a new object.
     */
    protected Database(String name) {
        Extension ext = Extensions.getExtension("jdbc.database", name);
        if (ext == null) {
            throw Exceptions.handle()
                            .to(Databases.LOG)
                            .withSystemErrorMessage("Unknown JDBC database: %s", name)
                            .handle();
        }
        Extension profile = Extensions.getExtension("jdbc.profile", ext.get("profile").asString("default"));
        Context ctx = profile.getContext();
        ctx.putAll(ext.getContext());
        this.name = name;
        this.driver = ext.get(KEY_DRIVER).isEmptyString() ?
                      Formatter.create(profile.get(KEY_DRIVER).asString()).set(ctx).format() :
                      ext.get(KEY_DRIVER).asString();
        this.url = ext.get(KEY_URL).isEmptyString() ?
                   Formatter.create(profile.get(KEY_URL).asString()).set(ctx).format() :
                   ext.get(KEY_URL).asString();
        this.username = ext.get(KEY_USER).isEmptyString() ?
                        Formatter.create(profile.get(KEY_USER).asString()).set(ctx).format() :
                        ext.get(KEY_USER).asString();
        this.password = ext.get(KEY_PASSWORD).isEmptyString() ?
                        Formatter.create(profile.get(KEY_PASSWORD).asString()).set(ctx).format() :
                        ext.get(KEY_PASSWORD).asString();
        this.initialSize = ext.get(KEY_INITIAL_SIZE).isFilled() ?
                           ext.get(KEY_INITIAL_SIZE).asInt(0) :
                           profile.get(KEY_INITIAL_SIZE).asInt(0);
        this.maxActive = ext.get(KEY_MAX_ACTIVE).isFilled() ?
                         ext.get(KEY_MAX_ACTIVE).asInt(10) :
                         profile.get(KEY_MAX_ACTIVE).asInt(10);
        this.maxIdle =
                ext.get(KEY_MAX_IDLE).isFilled() ? ext.get(KEY_MAX_IDLE).asInt(1) : profile.get(KEY_MAX_IDLE).asInt(1);
        this.validationQuery = ext.get(KEY_VALIDATION_QUERY).isEmptyString() ?
                               Formatter.create(profile.get(KEY_VALIDATION_QUERY).asString()).set(ctx).format() :
                               ext.get(KEY_VALIDATION_QUERY).asString();
        this.testOnBorrow = Strings.isFilled(validationQuery);
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
        if (ds == null) {
            ds = new MonitoredDataSource();
            initialize();
        }
        return ds;
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
        Transaction txn = getTransaction();
        if (txn != null) {
            return txn;
        } else {
            return createConnection();
        }
    }

    private Connection createConnection() throws SQLException {
        try (Operation op = new Operation(() -> "Database: " + name + ".getConnection()", Duration.ofSeconds(5))) {
            return new WrappedConnection(getDatasource().getConnection(), this);
        }
    }

    @Nullable
    protected Transaction getTransaction() throws SQLException {
        List<Transaction> transactions = TransactionContext.getTransactionStack(name);
        if (transactions.isEmpty()) {
            return null;
        } else {
            return transactions.get(transactions.size() - 1);
        }
    }

    protected Transaction startTransaction() throws SQLException {
        Databases.LOG.FINE("BEGIN " + name);
        Transaction txn = new Transaction(new WrappedConnection(createConnection(), this));
        List<Transaction> transactions = TransactionContext.getTransactionStack(name);
        transactions.add(txn);
        return txn;
    }

    /**
     * Starts a new transaction which can either be committed ({@link #commit()}) or rolled back ({@link #rollback()}.
     *
     * @throws SQLException in case of an database error
     */
    public void begin() throws SQLException {
        startTransaction();
    }

    /**
     * Joins an already running transaction or starts a new one if non is present.
     *
     * @return the joined or started transaction
     * @throws SQLException in case of an database error
     */
    public Transaction join() throws SQLException {
        List<Transaction> transactions = TransactionContext.getTransactionStack(name);
        if (transactions.isEmpty()) {
            begin();
            return transactions.get(0);
        } else {
            Transaction result = transactions.get(transactions.size() - 1).copy();
            transactions.add(result);
            return result;
        }
    }

    /**
     * Tries to commit a transaction. If none is present, nothing will happen.
     *
     * @throws SQLException in case of an database error
     */
    public void tryCommit() throws SQLException {
        List<Transaction> transactions = TransactionContext.getTransactionStack(name);
        if (transactions != null && !transactions.isEmpty()) {
            Transaction txn = transactions.get(transactions.size() - 1);
            transactions.remove(transactions.size() - 1);
            txn.tryCommit();
        }
    }

    /**
     * Commits the current transaction. Fails if none is present.
     *
     * @throws SQLException in case of an database error
     */
    public void commit() throws SQLException {
        List<Transaction> transactions = TransactionContext.getTransactionStack(name);
        if (transactions == null || transactions.isEmpty()) {
            throw new SQLException("Cannot commit a transaction: No transaction active!");
        } else {
            Transaction txn = transactions.get(transactions.size() - 1);
            transactions.remove(transactions.size() - 1);
            txn.commit();
        }
    }

    /**
     * Cancels (rolls back) the current transaction.
     */
    public void rollback() {
        List<Transaction> transactions = TransactionContext.getTransactionStack(name);
        if (transactions == null || transactions.isEmpty()) {
            return;
        }

        // Rollback this and all joined (copied) transactions
        HandledException ex = null;
        int lastIndex = transactions.size() - 1;
        for (int idx = lastIndex; idx >= 0; idx--) {
            try {
                Transaction txn = transactions.get(idx);
                txn.rollback();
                if (!txn.isCopy()) {
                    break;
                }
            } catch (Exception e) {
                ex = Exceptions.handle()
                               .to(Databases.LOG)
                               .error(e)
                               .withSystemErrorMessage("Error while rolling back transaction: %s (%s)")
                               .handle();
            }
        }
        transactions.remove(lastIndex);
        if (ex != null) {
            throw ex;
        }
    }

    /**
     * Performs the given task in the current transaction. If none is present, a new one will be started.
     *
     * @param r the code to execute within a transaction
     */
    public void transaction(Runnable r) {
        try {
            join();
            r.run();
            tryCommit();
        } catch (Exception t) {
            try {
                rollback();
            } catch (Exception e) {
                Exceptions.handle()
                          .to(Databases.LOG)
                          .error(e)
                          .withSystemErrorMessage("Error while rolling back a transaction: %s (%s)")
                          .handle();
            }
            throw Exceptions.handle()
                            .to(Databases.LOG)
                            .error(t)
                            .withSystemErrorMessage("Error while executing a transaction: %s (%s)")
                            .handle();
        }
    }

    /**
     * Performs the given code in its own transaction.
     *
     * @param r the code to execute
     * @throws SQLException in case of an database error
     */
    public void separateTransaction(Runnable r) throws SQLException {
        try {
            begin();
            r.run();
            tryCommit();
        } catch (Exception t) {
            try {
                rollback();
            } catch (Exception e) {
                Exceptions.handle()
                          .to(Databases.LOG)
                          .error(e)
                          .withSystemErrorMessage("Error while rolling back a transaction: %s (%s)")
                          .handle();
            }
            throw Exceptions.handle()
                            .to(Databases.LOG)
                            .error(t)
                            .withSystemErrorMessage("Error while executing a transaction: %s (%s)")
                            .handle();
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
     * @param fun        name of the function to call
     * @param returnType the SQL type ({@link Types}) of the return value of this function
     * @return a new call which can be supplied with parameters and executed against the database
     */
    public SQLCall createFunctionCall(String fun, Integer returnType) {
        return new SQLCall(this, fun, returnType);
    }

    /**
     * Creates a new call wrapper which permits safe and convenient procedure calls.
     *
     * @param fun name of the procedure to call
     * @return a new call which can be supplied with parameters and executed against the database
     */
    public SQLCall createProcedureCall(String fun) {
        return new SQLCall(this, fun, null);
    }

    /**
     * Generates an INSERT statement for the given table inserting all supplied parameters in <tt>ctx</tt>.
     *
     * @param table the target table to insert a row
     * @param ctx   contains names and values to insert into the database
     * @return a Row containing all generated keys
     * @throws SQLException in case of a database error
     */
    public Row insertRow(String table, Context ctx) throws SQLException {
        try (Connection c = getConnection()) {
            StringBuilder fields = new StringBuilder();
            StringBuilder values = new StringBuilder();
            List<Object> valueList = Lists.newArrayList();
            prepareValues(ctx, fields, values, valueList);
            String sql = "INSERT INTO " + table + " (" + fields + ") VALUES(" + values + ")";
            try (PreparedStatement stmt = c.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                fillValues(valueList, sql, stmt);
                stmt.executeUpdate();
                return SQLQuery.fetchGeneratedKeys(stmt);
            }
        }
    }

    protected void fillValues(List<Object> valueList, String sql, PreparedStatement stmt) {
        int index = 1;
        for (Object o : valueList) {
            try {
                stmt.setObject(index++, o);
            } catch (Exception e) {
                throw new IllegalArgumentException(e.getMessage()
                                                   + " - Index: "
                                                   + index
                                                   + ", Value: "
                                                   + o
                                                   + ", Query: "
                                                   + sql, e);
            }
        }
    }

    protected void prepareValues(Context ctx, StringBuilder fields, StringBuilder values, List<Object> valueList) {
        for (Map.Entry<String, Object> entry : ctx.entrySet()) {
            if (entry.getValue() != null) {
                if (fields.length() > 0) {
                    fields.append(", ");
                    values.append(", ");
                }
                fields.append(entry.getKey());
                values.append("?");
                valueList.add(Databases.convertValue(entry.getValue()));
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
        if (ds != null) {
            ds.setDriverClassName(driver);
            ds.setUrl(url);
            ds.setUsername(username);
            ds.setPassword(password);
            ds.setInitialSize(initialSize);
            ds.setMaxTotal(maxActive == 0 ? 20 : maxActive);
            ds.setMaxIdle(maxIdle);
            ds.setTestOnBorrow(testOnBorrow);
            ds.setValidationQuery(validationQuery);
            ds.setMaxWaitMillis(1000);
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

    public String getUsername() {
        return username;
    }

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
        if (ds == null) {
            return 0;
        }
        return ds.getNumIdle();
    }

    /**
     * Return the number of active connections
     *
     * @return the number of open and active connection
     */
    public int getNumActive() {
        if (ds == null) {
            return 0;
        }
        return ds.getNumActive();
    }

    /**
     * Determines if the current driver has the requested capability.
     *
     * @param cap the capability to determine
     * @return <tt>true</tt> if the capability is supported / present, <tt>false</tt> otherwise
     */
    public boolean hasCapability(Capability cap) {
        if (capabilities == null) {
            if ("com.mysql.jdbc.Driver".equalsIgnoreCase(driver)) {
                capabilities = Capability.MYSQL_CAPABILITIES;
            } else if ("org.hsqldb.jdbc.JDBCDriver".equalsIgnoreCase(driver)) {
                capabilities = Capability.HSQLDB_CAPABILITIES;
            } else if ("org.postgresql.Driver".equalsIgnoreCase(driver)) {
                capabilities = Capability.POSTGRES_CAPABILITIES;
            } else {
                capabilities = EnumSet.noneOf(Capability.class);
            }
        }

        return capabilities.contains(cap);
    }

    @Override
    public String toString() {
        return Strings.apply("%s (%d/%d)", name, getNumActive(), getSize());
    }
}
