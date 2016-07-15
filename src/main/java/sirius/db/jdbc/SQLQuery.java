/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import sirius.kernel.async.TaskContext;
import sirius.kernel.commons.Context;
import sirius.kernel.commons.Limit;
import sirius.kernel.commons.ValueHolder;
import sirius.kernel.commons.Watch;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

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
public class SQLQuery {

    private final Database ds;
    private final String sql;
    private List<String> fieldNames;
    private Context params = Context.create();

    /*
     * Create a new instance using Databases.createQuery(sql)
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
     * Executes the given query returning the result as list
     *
     * @return a list of {@link Row}s
     * @throws SQLException in case of a database error
     */
    @Nonnull
    public List<Row> queryList() throws SQLException {
        return queryList(Limit.UNLIMITED);
    }

    /**
     * Executes the given query returning the result as list with at most <tt>maxRows</tt> entries
     *
     * @param limit the limit which controls which and how many rows are output
     * @return a list of {@link Row}s
     * @throws SQLException in case of a database error
     */
    @Nonnull
    public List<Row> queryList(Limit limit) throws SQLException {
        List<Row> result = Lists.newArrayList();
        iterate(result::add, limit);

        return result;
    }

    /**
     * Executes the given query by invoking the given <tt>handler</tt> for each result row.
     * <p>
     * Consider using the method instead of {@link #queryList()} if a large result set is expected as this method. As
     * this method only processes one row at a time, this might be much more memory efficient.
     *
     * @param handler the row handler invoked for each row
     * @param limit   the limit which controls which and how many rows are output. Can be <tt>null</tt> to indicate
     *                that there is no limit.
     * @throws SQLException in case of a database error
     */
    public void iterate(Function<Row, Boolean> handler, @Nullable Limit limit) throws SQLException {
        Watch w = Watch.start();
        fieldNames = null;
        try (Connection c = ds.getConnection()) {
            try (PreparedStatement stmt = createPreparedStatement(c)) {
                if (stmt == null) {
                    return;
                }
                if (limit == null) {
                    limit = Limit.UNLIMITED;
                }
                if (limit.getTotalItems() > 0) {
                    stmt.setMaxRows(limit.getTotalItems());
                }
                if (ds.hasCapability(Capability.STREAMING)) {
                    stmt.setFetchSize(Integer.MIN_VALUE);
                } else {
                    stmt.setFetchSize(1000);
                }
                try (ResultSet rs = stmt.executeQuery()) {
                    TaskContext tc = TaskContext.get();
                    while (rs.next() && tc.isActive()) {
                        Row row = loadIntoRow(rs);
                        if (limit.nextRow()) {
                            if (!handler.apply(row)) {
                                break;
                            }
                        }
                        if (!limit.shouldContinue()) {
                            break;
                        }
                    }
                }
            }
        } finally {
            w.submitMicroTiming("SQL-QUERY", sql);
        }
    }

    protected PreparedStatement createPreparedStatement(Connection c) throws SQLException {
        StatementCompiler compiler = new StatementCompiler(c, false);
        compiler.buildParameterizedStatement(sql, params);
        return compiler.getStmt();
    }

    /**
     * Executes the given query by invoking the {@link Consumer} for each
     * result row.
     *
     * @param consumer the row handler invoked for each row
     * @param limit    the limit which controls which and how many rows are output. Can be <tt>null</tt> to indicate
     *                 that there is no limit.
     * @throws SQLException in case of a database error
     */
    public void iterateAll(Consumer<Row> consumer, @Nullable Limit limit) throws SQLException {
        iterate(r -> {
            consumer.accept(r);
            return true;
        }, limit);
    }

    /**
     * Executes the given query returning the first matching row wrapped as {@link java.util.Optional}.
     * <p>
     * This method behaves like {@link #queryFirst()} but returns an optional value instead of <tt>null</tt>.
     *
     * @return the resulting row wrapped as optional, or an empty optional if no matching row was found.
     * @throws SQLException in case of a database error
     */
    @Nonnull
    public Optional<Row> first() throws SQLException {
        return Optional.ofNullable(queryFirst());
    }

    /**
     * Executes the given query returning the first matching row.
     * <p>
     * If the resulting row contains a {@link Blob} an {@link OutputStream} as to be passed in as parameter
     * with the name name as the column. The contents of the blob will then be written into the given
     * output stream (without closing it).
     *
     * @return the first matching row for the given query or <tt>null</tt> if no matching row was found
     * @throws SQLException in case of a database error
     */
    @Nullable
    public Row queryFirst() throws SQLException {
        ValueHolder<Row> result = ValueHolder.of(null);
        iterateAll(result, Limit.singleItem());

        return result.get();
    }

    /*
     * Converts the current row of the given result set into a Row object
     */
    protected Row loadIntoRow(ResultSet rs) throws SQLException {
        Row row = new Row();
        List<String> fetchedFieldNames = null;
        if (fieldNames == null) {
            fetchedFieldNames = Lists.newArrayListWithCapacity(rs.getMetaData().getColumnCount());
        }
        for (int col = 1; col <= rs.getMetaData().getColumnCount(); col++) {
            String fieldName = null;
            if (fieldNames != null) {
                fieldName = fieldNames.get(col - 1);
            } else {
                fieldName = rs.getMetaData().getColumnLabel(col).toUpperCase();
                fetchedFieldNames.add(fieldName);
            }
            Object obj = rs.getObject(col);
            if (obj instanceof Blob) {
                writeBlobToParameter(fieldName, rs, col, (Blob) obj);
            } else {
                row.fields.put(fieldName, obj);
            }
        }

        if (fieldNames == null) {
            fieldNames = Collections.unmodifiableList(fetchedFieldNames);
        }
        row.fieldNames = fieldNames;

        return row;
    }

    /*
     * If a Blob is inside a result set, we expect an OutputStream as parameter with the same name which we write
     * the data to.
     */
    protected void writeBlobToParameter(String name, ResultSet rs, int col, Blob blob) throws SQLException {
        OutputStream out = (OutputStream) params.get(name);
        if (out != null) {
            try {
                try (InputStream in = blob.getBinaryStream()) {
                    ByteStreams.copy(in, out);
                }
            } catch (IOException e) {
                throw new SQLException(e);
            }
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
            w.submitMicroTiming("SQL", sql);
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
                return fetchGeneratedKeys(stmt);
            }
        } finally {
            w.submitMicroTiming("SQL-QUERY", sql);
        }
    }

    /**
     * Returns all generated keys wrapped as row
     *
     * @param stmt the statement which was used to perform an update or insert
     * @return a row containing all generated keys
     * @throws SQLException in case of an error thrown by the database or driver
     */
    protected static Row fetchGeneratedKeys(PreparedStatement stmt) throws SQLException {
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

    @Override
    public String toString() {
        return "SQLQuery [" + sql + "]";
    }
}
