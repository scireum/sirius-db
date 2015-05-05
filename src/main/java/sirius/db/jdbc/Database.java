/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import com.google.common.collect.Lists;
import org.apache.commons.dbcp.BasicDataSource;
import sirius.kernel.async.Operation;
import sirius.kernel.commons.Context;
import sirius.kernel.commons.Strings;
import sirius.kernel.extensions.Extension;
import sirius.kernel.extensions.Extensions;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.HandledException;
import sirius.kernel.nls.Formatter;
import sirius.mixing.OMA;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.*;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/**
 * Represents a database connection obtained via {@link Databases#get(String)}.
 * <p>
 * Use {@link #createQuery(String)} to create an SQL query with built in connection management.
 * Use {@link #getConnection()} to obtain a regular JDBC connection (which has to be handled with some caution).
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2013/11
 */
public class Database {

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
    private BasicDataSource ds;
    private EnumSet<Capability> capabilities;

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
        this.driver = ext.get("driver").isEmptyString() ? Formatter.create(profile.get("driver").asString())
                                                                   .set(ctx)
                                                                   .format() : ext.get("driver").asString();
        this.url = ext.get("url").isEmptyString() ? Formatter.create(profile.get("url").asString())
                                                             .set(ctx)
                                                             .format() : ext.get("url").asString();
        this.username = ext.get("user").isEmptyString() ? Formatter.create(profile.get("user").asString())
                                                                   .set(ctx)
                                                                   .format() : ext.get("user").asString();
        this.password = ext.get("password").isEmptyString() ? Formatter.create(profile.get("password").asString())
                                                                       .set(ctx)
                                                                       .format() : ext.get("password").asString();
        this.initialSize = ext.get("initialSize").isFilled() ? ext.get("initialSize").asInt(0) : profile.get(
                "initialSize").asInt(0);
        this.maxActive = ext.get("maxActive").isFilled() ? ext.get("maxActive").asInt(10) : profile.get("maxActive")
                                                                                                   .asInt(10);
        this.maxIdle = ext.get("maxIdle").isFilled() ? ext.get("maxIdle").asInt(1) : profile.get("maxIdle").asInt(1);
        this.validationQuery = ext.get("validationQuery").isEmptyString() ? Formatter.create(profile.get(
                "validationQuery").asString()).set(ctx).format() : ext.get("validationQuery").asString();
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
            ds = new BasicDataSource();
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
        Operation op = Operation.create("sql", () -> "Database: " + name + ".getConnection()", Duration.ofSeconds(5));
        try {
            return new WrappedConnection(getDatasource().getConnection(), this);
        } finally {
            Operation.release(op);
        }
    }

    @Nullable
    protected Transaction getTransaction() throws SQLException {
        List<Transaction> transactions = TransactionManager.getTransactionStack(name);
        if (transactions.isEmpty()) {
            return null;
        } else {
            return transactions.get(transactions.size() - 1);
        }
    }

    protected Transaction startTransaction() throws SQLException {
        Databases.LOG.FINE("BEGIN " + name);
        Transaction txn = new Transaction(new WrappedConnection(createConnection(), this));
        List<Transaction> transactions = TransactionManager.getTransactionStack(name);
        transactions.add(txn);
        return txn;
    }

    public void begin() throws SQLException {
        startTransaction();
    }

    public Transaction join() throws SQLException {
        List<Transaction> transactions = TransactionManager.getTransactionStack(name);
        if (transactions.isEmpty()) {
            begin();
            return transactions.get(0);
        } else {
            Transaction result = transactions.get(transactions.size() - 1).copy();
            transactions.add(result);
            return result;
        }
    }

    public void tryCommit() throws SQLException {
        List<Transaction> transactions = TransactionManager.getTransactionStack(name);
        if (transactions == null || transactions.isEmpty()) {
            return;
        } else {
            Transaction txn = transactions.get(transactions.size() - 1);
            transactions.remove(transactions.size() - 1);
            txn.tryCommit();
        }
    }

    public void commit() throws SQLException {
        List<Transaction> transactions = TransactionManager.getTransactionStack(name);
        if (transactions == null || transactions.isEmpty()) {
            throw new SQLException("Cannot commit a transaction: No transaction active!");
        } else {
            Transaction txn = transactions.get(transactions.size() - 1);
            transactions.remove(transactions.size() - 1);
            txn.commit();
        }
    }

    public void rollback() throws SQLException {
        List<Transaction> transactions = TransactionManager.getTransactionStack(name);
        if (transactions == null || transactions.isEmpty()) {
            throw new SQLException("Cannot rollback a transaction: No transaction active!");
        } else {
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
                } catch (SQLException e) {
                    ex = Exceptions.handle()
                                   .to(OMA.LOG)
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
    }

    public void transaction(Runnable r) throws SQLException {
        join();
        try {
            r.run();
        } finally {
            tryCommit();
        }
    }

    public void separateTransaction(Runnable r) throws SQLException {
        begin();
        try {
            r.run();
        } finally {
            tryCommit();
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
            for (Map.Entry<String, Object> entry : ctx.entrySet()) {
                if (entry.getValue() != null) {
                    if (fields.length() > 0) {
                        fields.append(", ");
                        values.append(", ");
                    }
                    fields.append(entry.getKey());
                    values.append("?");
                    valueList.add(entry.getValue());
                }
            }
            String sql = "INSERT INTO " + table + " (" + fields.toString() + ") VALUES(" + values + ")";
            try (PreparedStatement stmt = c.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                int index = 1;
                for (Object o : valueList) {
                    try {
                        stmt.setObject(index++, o);
                    } catch (Exception e) {
                        throw new IllegalArgumentException(e.getMessage() + " - Index: " + index + ", Value: " + o + ", Query: " + sql,
                                                           e);
                    }
                }
                stmt.executeUpdate();
                try (ResultSet rs = stmt.getGeneratedKeys()) {
                    Row row = new Row();
                    if (rs.next()) {
                        for (int col = 1; col <= rs.getMetaData().getColumnCount(); col++) {
                            row.fields.put(rs.getMetaData().getColumnLabel(col), rs.getObject(col));
                        }
                    }
                    return row;
                }
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
            ds.setMaxActive(maxActive == 0 ? 20 : maxActive);
            ds.setMaxIdle(maxIdle);
            ds.setTestOnBorrow(testOnBorrow);
            ds.setValidationQuery(validationQuery);
            ds.setMaxWait(1000);
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
