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
import sirius.kernel.commons.Limit;
import sirius.kernel.commons.Tuple;
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
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class BaseSQLQuery {

    protected List<String> fieldNames;

    /**
     * Returns all generated keys wrapped as row
     *
     * @param stmt the statement which was used to perform an update or insert
     * @return a row containing all generated keys
     * @throws SQLException in case of an error thrown by the database or driver
     */
    public static Row fetchGeneratedKeys(PreparedStatement stmt) throws SQLException {
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
    public abstract void iterate(Function<Row, Boolean> handler, @Nullable Limit limit) throws SQLException;

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

    protected void processResultSet(Function<Row, Boolean> handler,
                                    Limit effectiveLimit,
                                    ResultSet resultSet,
                                    TaskContext taskContext) throws SQLException {
        while (resultSet.next() && taskContext.isActive()) {
            Row row = loadIntoRow(resultSet);
            if (effectiveLimit.nextRow()) {
                if (!handler.apply(row)) {
                    return;
                }
            }
            if (!effectiveLimit.shouldContinue()) {
                return;
            }
        }
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
            String fieldName;
            if (fieldNames != null) {
                fieldName = fieldNames.get(col - 1);
            } else {
                fieldName = rs.getMetaData().getColumnLabel(col);
                fetchedFieldNames.add(fieldName);
            }
            Object obj = rs.getObject(col);
            if (obj instanceof Blob) {
                writeBlobToParameter(fieldName, (Blob) obj);
            } else {
                row.fields.put(fieldName.toUpperCase(), Tuple.create(fieldName, obj));
            }
        }

        if (fieldNames == null) {
            fieldNames = Collections.unmodifiableList(fetchedFieldNames);
        }

        return row;
    }

    /*
     * If a Blob is inside a result set, we expect an OutputStream as parameter with the same name which we write
     * the data to.
     */
    protected abstract void writeBlobToParameter(String name, Blob blob) throws SQLException;

}
