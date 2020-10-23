/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.DB;
import sirius.kernel.async.ExecutionPoint;
import sirius.kernel.async.Operation;
import sirius.kernel.commons.Explain;
import sirius.kernel.commons.Watch;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Calendar;

/**
 * Wrapper for {@link PreparedStatement} to add microtiming.
 */
class WrappedPreparedStatement implements PreparedStatement {

    private static final Duration LONG_RUNNING_QUERY_OPERATION = Duration.ofMinutes(15);
    private static final Duration QUERY_OPERATION = Duration.ofSeconds(30);

    private PreparedStatement delegate;
    private final String preparedSQL;
    private boolean longRunning;

    WrappedPreparedStatement(PreparedStatement preparedStatement, boolean longRunning, String preparedSQL) {
        this.delegate = preparedStatement;
        this.longRunning = longRunning;
        this.preparedSQL = preparedSQL;
    }

    protected void updateStatistics(String sql, Watch w) {
        w.submitMicroTiming("SQL","PreparedStatement: " + sql);
        Databases.numQueries.inc();
        if (!longRunning) {
            Databases.queryDuration.addValue(w.elapsedMillis());
            if (w.elapsedMillis() > Databases.getLogQueryThresholdMillis()) {
                Databases.numSlowQueries.inc();
                DB.SLOW_DB_LOG.INFO("A slow JDBC query was executed (%s): %s\n%s",
                                    w.duration(),
                                    sql,
                                    ExecutionPoint.snapshot().toString());
            }
        }
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (Databases.LOG.isFINE()) {
            Databases.LOG.FINE(sql);
        }
        Watch w = Watch.start();
        try (Operation op = new Operation(() -> sql, determineOperationDuration())) {
            return delegate.executeQuery(sql);
        } finally {
            updateStatistics(sql, w);
        }
    }

    private Duration determineOperationDuration() {
        return longRunning ? LONG_RUNNING_QUERY_OPERATION : QUERY_OPERATION;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return delegate.unwrap(iface);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        if (Databases.LOG.isFINE()) {
            Databases.LOG.FINE(preparedSQL);
        }
        Watch w = Watch.start();
        try (Operation op = new Operation(() -> preparedSQL, determineOperationDuration())) {
            return delegate.executeQuery();
        } finally {
            updateStatistics(preparedSQL, w);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        if (Databases.LOG.isFINE()) {
            Databases.LOG.FINE(sql);
        }
        Watch w = Watch.start();
        try (Operation op = new Operation(() -> sql, determineOperationDuration())) {
            return delegate.executeUpdate(sql);
        } finally {
            updateStatistics(sql, w);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return delegate.isWrapperFor(iface);
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (Databases.LOG.isFINE()) {
            Databases.LOG.FINE(preparedSQL);
        }
        Watch w = Watch.start();
        try (Operation op = new Operation(() -> preparedSQL, determineOperationDuration())) {
            return delegate.executeUpdate();
        } finally {
            updateStatistics(preparedSQL, w);
        }
    }

    @Override
    public void close() throws SQLException {
        delegate.close();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        delegate.setNull(parameterIndex, sqlType);
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return delegate.getMaxFieldSize();
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        delegate.setBoolean(parameterIndex, x);
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        delegate.setMaxFieldSize(max);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        delegate.setByte(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        delegate.setShort(parameterIndex, x);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return delegate.getMaxRows();
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        delegate.setInt(parameterIndex, x);
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        delegate.setMaxRows(max);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        delegate.setLong(parameterIndex, x);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        delegate.setEscapeProcessing(enable);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        delegate.setFloat(parameterIndex, x);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return delegate.getQueryTimeout();
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        delegate.setDouble(parameterIndex, x);
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        delegate.setQueryTimeout(seconds);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        delegate.setBigDecimal(parameterIndex, x);
    }

    @Override
    public void cancel() throws SQLException {
        delegate.cancel();
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        delegate.setString(parameterIndex, x);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return delegate.getWarnings();
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        delegate.setBytes(parameterIndex, x);
    }

    @Override
    public void clearWarnings() throws SQLException {
        delegate.clearWarnings();
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        delegate.setDate(parameterIndex, x);
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        delegate.setCursorName(name);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        delegate.setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        delegate.setTimestamp(parameterIndex, x);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        if (Databases.LOG.isFINE()) {
            Databases.LOG.FINE(sql);
        }
        Watch w = Watch.start();
        try (Operation op = new Operation(() -> sql, determineOperationDuration())) {
            return delegate.execute(sql);
        } finally {
            updateStatistics(sql, w);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        delegate.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return delegate.getResultSet();
    }

    /**
     * Sets the designated parameter to the given input stream, which
     * will have the specified number of bytes.
     * <p>
     * When a very large Unicode value is input to a {@code LONGVARCHAR}
     * parameter, it may be more practical to send it via a
     * {@code java.io.InputStream} object. The data will be read from the
     * stream as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from Unicode to the database char format.
     * <p>
     * The byte format of the Unicode stream must be a Java UTF-8, as defined in the
     * Java Virtual Machine Specification.
     * <p>
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              a {@code java.io.InputStream} object that contains the
     *                       Unicode parameter value
     * @param length         the number of bytes in the stream
     * @throws SQLException if parameterIndex does not correspond to a parameter
     *                      marker in the SQL statement; if a database access error occurs or
     *                      this method is called on a closed {@code PreparedStatement}
     * @deprecated Use {@code setCharacterStream}
     */
    @Override
    @Deprecated
    @SuppressWarnings("squid:S1133")
    @Explain("We cannot change a Java core API")
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        delegate.setUnicodeStream(parameterIndex, x, length);
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return delegate.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return delegate.getMoreResults();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        delegate.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        delegate.setFetchDirection(direction);
    }

    @Override
    public void clearParameters() throws SQLException {
        delegate.clearParameters();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return delegate.getFetchDirection();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        delegate.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        delegate.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return delegate.getFetchSize();
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        delegate.setObject(parameterIndex, x);
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return delegate.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return delegate.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        delegate.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        delegate.clearBatch();
    }

    @Override
    public boolean execute() throws SQLException {
        if (Databases.LOG.isFINE()) {
            Databases.LOG.FINE(preparedSQL);
        }
        Watch w = Watch.start();
        try (Operation op = new Operation(() -> preparedSQL, Duration.ofSeconds(30))) {
            return delegate.execute();
        } finally {
            updateStatistics(preparedSQL, w);
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        Watch w = Watch.start();
        try (Operation op = new Operation(() -> "executeBatch: " + preparedSQL, determineOperationDuration())) {
            int[] result = delegate.executeBatch();
            w.submitMicroTiming("SQL","Batch: " + preparedSQL);
            Databases.numQueries.inc();
            if (!longRunning) {
                Databases.queryDuration.addValue(w.elapsedMillis());
                if (w.elapsedMillis() > Databases.getLogQueryThresholdMillis()) {
                    Databases.numSlowQueries.inc();
                    DB.SLOW_DB_LOG.INFO("A slow JDBC batch query was executed (%s): %s (%s rows)\n%s",
                                        w.duration(),
                                        preparedSQL,
                                        result.length,
                                        ExecutionPoint.snapshot().toString());
                }
            }

            return result;
        }
    }

    @Override
    public void addBatch() throws SQLException {
        delegate.addBatch();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        delegate.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        delegate.setRef(parameterIndex, x);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return delegate.getConnection();
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        delegate.setBlob(parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        delegate.setClob(parameterIndex, x);
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return delegate.getMoreResults(current);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        delegate.setArray(parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return delegate.getMetaData();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return delegate.getGeneratedKeys();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        delegate.setDate(parameterIndex, x, cal);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        if (Databases.LOG.isFINE()) {
            Databases.LOG.FINE(sql);
        }
        Watch w = Watch.start();
        try (Operation op = new Operation(() -> sql, determineOperationDuration())) {
            return delegate.executeUpdate(sql, autoGeneratedKeys);
        } finally {
            updateStatistics(sql, w);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        delegate.setTime(parameterIndex, x, cal);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        if (Databases.LOG.isFINE()) {
            Databases.LOG.FINE(sql);
        }
        Watch w = Watch.start();
        try (Operation op = new Operation(() -> sql, determineOperationDuration())) {
            return delegate.executeUpdate(sql, columnIndexes);
        } finally {
            updateStatistics(sql, w);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        delegate.setTimestamp(parameterIndex, x, cal);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        delegate.setNull(parameterIndex, sqlType, typeName);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        if (Databases.LOG.isFINE()) {
            Databases.LOG.FINE(sql);
        }
        Watch w = Watch.start();
        try (Operation op = new Operation(() -> sql, determineOperationDuration())) {
            return delegate.executeUpdate(sql, columnNames);
        } finally {
            updateStatistics(sql, w);
        }
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        if (Databases.LOG.isFINE()) {
            Databases.LOG.FINE(sql);
        }
        Watch w = Watch.start();
        try (Operation op = new Operation(() -> sql, determineOperationDuration())) {
            return delegate.execute(sql, autoGeneratedKeys);
        } finally {
            updateStatistics(sql, w);
        }
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        delegate.setURL(parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return delegate.getParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        delegate.setRowId(parameterIndex, x);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        if (Databases.LOG.isFINE()) {
            Databases.LOG.FINE(sql);
        }
        Watch w = Watch.start();
        try (Operation op = new Operation(() -> sql, determineOperationDuration())) {
            return delegate.execute(sql, columnIndexes);
        } finally {
            updateStatistics(sql, w);
        }
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        delegate.setNString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        delegate.setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        if (Databases.LOG.isFINE()) {
            Databases.LOG.FINE(sql);
        }
        Watch w = Watch.start();
        try (Operation op = new Operation(() -> sql, determineOperationDuration())) {
            return delegate.execute(sql, columnNames);
        } finally {
            updateStatistics(sql, w);
        }
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        delegate.setNClob(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        delegate.setClob(parameterIndex, reader, length);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return delegate.getResultSetHoldability();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        delegate.setBlob(parameterIndex, inputStream, length);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return delegate.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        delegate.setPoolable(poolable);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        delegate.setNClob(parameterIndex, reader, length);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return delegate.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        delegate.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return delegate.isCloseOnCompletion();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        delegate.setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        delegate.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        delegate.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        delegate.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        delegate.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        delegate.setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        delegate.setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        delegate.setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        delegate.setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        delegate.setClob(parameterIndex, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        delegate.setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        delegate.setNClob(parameterIndex, reader);
    }
}
