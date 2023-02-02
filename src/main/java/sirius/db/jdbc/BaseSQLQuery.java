/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.kernel.async.TaskContext;
import sirius.kernel.commons.Limit;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.ValueHolder;
import sirius.kernel.di.std.Part;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Abstract class to provide common functionality when executing a query and iterating over its result set.
 */
public abstract class BaseSQLQuery {

    @Part
    protected static Databases dbs;

    protected List<String> fieldNames;

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
        List<Row> result = new ArrayList<>();
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
    public abstract void iterate(Predicate<Row> handler, @Nullable Limit limit) throws SQLException;

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

    protected void processResultSet(Predicate<Row> handler,
                                    Limit effectiveLimit,
                                    ResultSet resultSet,
                                    TaskContext taskContext,
                                    boolean longRunning) throws SQLException {
        while (resultSet.next() && (taskContext.isActive() && longRunning)) {
            Row row = loadIntoRow(resultSet);
            if (effectiveLimit.nextRow() && !handler.test(row)) {
                return;
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
     * with the name as the column. The contents of the blob will then be written into the given
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
            fetchedFieldNames = new ArrayList<>(rs.getMetaData().getColumnCount());
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
            if (obj instanceof Blob blob) {
                writeBlobToParameter(fieldName, blob);
            } else if (obj instanceof String string) {
                row.fields.put(fieldName.toUpperCase(), Tuple.create(fieldName, string.replace("\0", "")));
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
