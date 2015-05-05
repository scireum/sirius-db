/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing;

import com.google.common.collect.Lists;
import sirius.kernel.async.BackgroundTaskQueue;
import sirius.kernel.di.std.Register;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;

/**
 * Created by aha on 01.05.15.
 */
@Register(classes = {CascadeDeleteTaskQueue.class, BackgroundTaskQueue.class})
public class CascadeDeleteTaskQueue implements BackgroundTaskQueue {

    private List<Runnable> referencesToCheck = Lists.newArrayList();
    private Iterator<Runnable> referencesIter;

    @Nonnull
    @Override
    public String getQueueName() {
        return "DB - Cascade Deletes";
    }

    @Nullable
    @Override
    public Runnable getWork() {
        synchronized (referencesToCheck) {
            if (referencesToCheck.isEmpty()) {
                return null;
            }
            if (referencesIter == null || !referencesIter.hasNext()) {
                referencesIter = referencesToCheck.iterator();
            }
            return referencesIter.next();
        }
    }

    public void addReferenceToCheck(Runnable check) {
        synchronized (referencesToCheck) {
            referencesToCheck.add(check);
            referencesIter = null;
        }
    }
}
