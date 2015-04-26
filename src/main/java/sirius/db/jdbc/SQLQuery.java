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
import java.sql.ResultSet;
import java.sql.SQLException;
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
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2013/11
 */
public class SQLQuery {

    private final Database ds;
    private final String sql;
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
        try (Connection c = ds.getConnection()) {
            StatementCompiler compiler = new StatementCompiler(c, false);
            compiler.buildParameterizedStatement(sql, params);
            if (compiler.getStmt() == null) {
                return;
            }
            if (limit == null) {
                limit = Limit.UNLIMITED;
            }
            if (limit.getMaxItems() > 0) {
                compiler.getStmt().setMaxRows(limit.getMaxItems());
            }
            if (ds.isMySQL() && (limit.getMaxItems() > 1000 || limit.getMaxItems() <= 0)) {
                compiler.getStmt().setFetchSize(Integer.MIN_VALUE);
            }
            try (ResultSet rs = compiler.getStmt().executeQuery()) {
                TaskContext tc = TaskContext.get();
                while (rs.next() && tc.isActive()) {
                    limit.nextRow();
                    Row row = loadIntoRow(rs);
                    if (limit.shouldOutput()) {
                        if (!handler.apply(row)) {
                            break;
                        }
                    }
                    if (!limit.shouldContinue()) {
                        break;
                    }
                }
            } finally {
                compiler.getStmt().close();
            }

        } finally {
            w.submitMicroTiming("SQL-QUERY", sql);
        }
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
        iterateAll(result, Limit.SINGLE_ITEM);

        return result.get();
    }

    /*
     * Converts the current row of the given result set into a Row object
     */
    private Row loadIntoRow(ResultSet rs) throws SQLException {
        Row row = new Row();
        for (int col = 1; col <= rs.getMetaData().getColumnCount(); col++) {
            Object obj = rs.getObject(col);
            if (obj instanceof Blob) {
                writeBlobToParameter(rs, col, (Blob) obj);
            } else {
                row.fields.put(rs.getMetaData().getColumnLabel(col).toUpperCase(), obj);
            }
        }
        return row;
    }

    /*
     * If a Blob is inside a result set, we expect an OutputStream as parameter with the same name which we write
     * the data to.
     */
    private void writeBlobToParameter(ResultSet rs, int col, Blob blob) throws SQLException {
        OutputStream out = (OutputStream) params.get(rs.getMetaData().getColumnLabel(col));
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
            StatementCompiler compiler = new StatementCompiler(c, false);
            compiler.buildParameterizedStatement(sql, params);
            if (compiler.getStmt() == null) {
                return 0;
            }
            try {
                return compiler.getStmt().executeUpdate();
            } finally {
                compiler.getStmt().close();
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
            if (compiler.getStmt() == null) {
                return new Row();
            }
            try {
                compiler.getStmt().executeUpdate();
                try (ResultSet rs = compiler.getStmt().getGeneratedKeys()) {
                    Row row = new Row();
                    if (rs.next()) {
                        for (int col = 1; col <= rs.getMetaData().getColumnCount(); col++) {
                            row.fields.put(rs.getMetaData().getColumnLabel(col), rs.getObject(col));
                        }
                    }
                    return row;
                }
            } finally {
                compiler.getStmt().close();
            }
        } finally {
            w.submitMicroTiming("SQL-QUERY", sql);
        }
    }

    @Override
    public String toString() {
        return "SQLQuery [" + sql + "]";
    }

}
