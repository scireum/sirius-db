/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.kernel.commons.Watch;

import java.sql.SQLException;

/**
 * Represents a database transaction managed by {@link Database#begin()}, {@link Database#join()} etc.
 */
public class Transaction extends DelegatingConnection<WrappedConnection> {
    private boolean copy;
    private boolean closed;
    private Watch watch = Watch.start();

    protected Transaction(WrappedConnection delegate) throws SQLException {
        super(delegate);
        setAutoCommit(false);
    }

    protected Transaction copy() throws SQLException {
        Transaction result = new Transaction(delegate);
        result.copy = true;
        return result;
    }

    protected boolean isCopy() {
        return copy;
    }

    @Override
    public void close() throws SQLException {
        // Transactions close on commit or rollback
    }

    protected void tryCommit() throws SQLException {
        if (copy) {
            return;
        }
        if (closed) {
            return;
        }
        Databases.LOG.FINE("COMMIT " + delegate.database.name);
        closed = true;
        delegate.commit();
    }

    @Override
    public void commit() throws SQLException {
        if (copy) {
            return;
        }
        if (closed) {
            throw new SQLException("Transaction has already been committed or rolled back");
        }
        Databases.LOG.FINE("COMMIT " + delegate.database.name);
        closed = true;
        delegate.commit();
    }

    @Override
    public void rollback() throws SQLException {
        if (copy || closed) {
            return;
        }
        Databases.LOG.FINE("ROLLBACK " + delegate.database.name);
        closed = true;
        delegate.rollback();
        watch.submitMicroTiming("SQL", "Transaction Duration: " + delegate.database.name);
    }

    @Override
    public String toString() {
        if (copy) {
            return "Transaction (copy): " + delegate.toString();
        } else {
            return "Transaction: " + delegate.toString();
        }
    }
}
