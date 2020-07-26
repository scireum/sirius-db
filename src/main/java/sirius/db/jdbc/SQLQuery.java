/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.kernel.async.TaskContext;
import sirius.kernel.commons.Context;
import sirius.kernel.commons.Limit;
import sirius.kernel.commons.Streams;
import sirius.kernel.commons.Watch;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Represents a flexible way of executing parameterized SQL queries without
 * thinking too much about resource management.
 * <p>
 * Supports named parameters in form of ${name}. Also #{name} can be used in LIKE expressions and will be
 * surrounded by % signs (if not empty).
 * <p>
 * Optional blocks can be surrounded with angular braces: SELECT * FROM x WHERE test = 1[ AND test2=${val}]
 * The surrounded block will only be added to the query, if the parameter within has a non-null value.
 */
public class SQLQuery extends BaseSQLQuery {

    /**
     * Specifies the default fetch size (number of rows to fetch at once) for a query.
     * <p>
     * Note that some databases (MySQL / Maria DB) no not support this. If possible ({@link Capability#STREAMING})
     * these are set to a minimal fethc size to enable streaming of large results.
     */
    public static final int DEFAULT_FETCH_SIZE = 1000;

    private static final String MICROTIMING_KEY = "SQL-QUERY";
    private final Database ds;
    private final String sql;
    private Context params = Context.create();
    private boolean longRunning;

    /**
     * Creates a new instance for the given datasource and query.
     * <p>
     * Create a new instance using {@code Databases.createQuery(sql)}.
     *
     * @param ds  the datasource to query
     * @param sql the query to execute
     */
    protected SQLQuery(Database ds, String sql) {
        this.ds = ds;
        this.sql = sql;
    }

    /**
     * Adds a parameter.
     *
     * @param parameter the name of the parameter as referenced in the SQL statement (${name} or #{name}).
     * @param value     the value of the parameter
     * @return the query itself to support fluent calls
     */
    public SQLQuery set(String parameter, Object value) {
        params.put(parameter, value);
        return this;
    }

    /**
     * Sets all parameters of the given context.
     *
     * @param ctx the containing pairs of names and values to add to the query
     * @return the query itself to support fluent calls
     */
    public SQLQuery set(Map<String, Object> ctx) {
        params.putAll(ctx);
        return this;
    }

    /**
     * Marks the connection and its statements as potentially long running.
     * <p>
     * These connections and statements won't contribute to the query duration metrics and will not report slow queries.
     * Note however, that each query is still wrapped in an {@link sirius.kernel.async.Operation} which is considered
     * as hanging after 5mins.
     *
     * @return the query itself for fluent method calls
     */
    public SQLQuery markAsLongRunning() {
        this.longRunning = true;
        return this;
    }

    @Override
    public void iterate(Predicate<Row> handler, @Nullable Limit limit) throws SQLException {
        Watch w = Watch.start();
        fieldNames = null;
        try (Connection c = longRunning ? ds.getLongRunningConnection() : ds.getConnection()) {
            try (PreparedStatement stmt = createPreparedStatement(c)) {
                if (stmt == null) {
                    return;
                }
                Limit effectiveLimit = limit != null ? limit : Limit.UNLIMITED;
                applyMaxRows(stmt, effectiveLimit);
                applyFetchSize(stmt, effectiveLimit);

                try (ResultSet rs = stmt.executeQuery()) {
                    w.submitMicroTiming(MICROTIMING_KEY, sql);
                    TaskContext tc = TaskContext.get();
                    processResultSet(handler, effectiveLimit, rs, tc);
                }
            }
        }
    }

    protected void applyMaxRows(PreparedStatement stmt, Limit effectiveLimit) throws SQLException {
        if (effectiveLimit.getTotalItems() > 0) {
            stmt.setMaxRows(effectiveLimit.getTotalItems());
        }
    }

    protected void applyFetchSize(PreparedStatement stmt, Limit effectiveLimit) throws SQLException {
        if (effectiveLimit.getTotalItems() > DEFAULT_FETCH_SIZE || effectiveLimit.getTotalItems() <= 0) {
            if (ds.hasCapability(Capability.STREAMING)) {
                stmt.setFetchSize(Integer.MIN_VALUE);
            } else {
                stmt.setFetchSize(DEFAULT_FETCH_SIZE);
            }
        }
    }

    protected PreparedStatement createPreparedStatement(Connection c) throws SQLException {
        StatementCompiler compiler = new StatementCompiler(c, false);
        compiler.buildParameterizedStatement(sql, params);
        return compiler.getStmt();
    }

    @Override
    protected void writeBlobToParameter(String name, Blob blob) throws SQLException {
        OutputStream out = (OutputStream) params.get(name);
        if (out == null) {
            return;
        }

        try (InputStream in = blob.getBinaryStream()) {
            Streams.transfer(in, out);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Executes the query as update.
     * <p>
     * Requires the SQL statement to be an UPDATE or DELETE statement.
     *
     * @return the number of rows changed
     * @throws SQLException in case of a database error
     */
    public int executeUpdate() throws SQLException {
        Watch w = Watch.start();

        try (Connection c = ds.getConnection()) {
            try (PreparedStatement stmt = createPreparedStatement(c)) {
                if (stmt == null) {
                    return 0;
                }
                return stmt.executeUpdate();
            }
        } finally {
            w.submitMicroTiming(MICROTIMING_KEY, sql);
        }
    }

    /**
     * Executes the update and returns the generated keys.
     * <p>
     * Requires the SQL statement to be an UPDATE or DELETE statement.
     *
     * @return the a row representing all generated keys
     * @throws SQLException in case of a database error
     */
    public Row executeUpdateReturnKeys() throws SQLException {
        Watch w = Watch.start();

        try (Connection c = ds.getConnection()) {
            StatementCompiler compiler = new StatementCompiler(c, true);
            compiler.buildParameterizedStatement(sql, params);
            try (PreparedStatement stmt = compiler.getStmt()) {
                if (stmt == null) {
                    return new Row();
                }
                stmt.executeUpdate();
                return dbs.fetchGeneratedKeys(stmt);
            }
        } finally {
            w.submitMicroTiming(MICROTIMING_KEY, sql);
        }
    }

    @Override
    public String toString() {
        return "SQLQuery [" + sql + "]";
    }
}
