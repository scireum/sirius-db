/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import sirius.kernel.async.CallContext;
import sirius.kernel.async.SubContext;
import sirius.kernel.health.Exceptions;

import java.util.List;
import java.util.Map;

/**
 * Manages {@link Transaction}s as sub context within {@link CallContext}.
 * <p>
 * Can be used to manage transactions accross method boundaries.
 *
 * @see Database#begin()
 * @see Database#join()
 */
public class TransactionContext implements SubContext {

    private Map<String, List<Transaction>> txns = Maps.newConcurrentMap();

    protected static List<Transaction> getTransactionStack(String database) {
        TransactionContext manager = CallContext.getCurrent().get(TransactionContext.class);
        List<Transaction> result = manager.txns.get(database);
        if (result == null) {
            result = Lists.newArrayList();
            manager.txns.put(database, result);
        }

        return result;
    }

    @Override
    public SubContext fork() {
        // Transactions are not shared accross threads...
        return new TransactionContext();
    }

    @Override
    public void detach() {
        for (Map.Entry<String, List<Transaction>> entry : txns.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                Exceptions.handle()
                          .to(Databases.LOG)
                          .withSystemErrorMessage("Thread '%s' left a transaction open for database '%s'",
                                                  Thread.currentThread().getName(),
                                                  entry.getKey())
                          .handle();
            }
        }
    }
}
