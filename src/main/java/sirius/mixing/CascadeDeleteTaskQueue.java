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
 * Constantly checks if there are unsatisfied foreign key constraints and removes the entities accordingly.
 * <p>
 * An {@link EntityRef} which has {@link sirius.mixing.EntityRef.OnDelete#LAZY_CASCADE} as delete handler does not
 * instantly remove the entity if its parent is deleted. Rather this background handler eventually picks it up and
 * deletes it. We also check references of type {@link sirius.mixing.EntityRef.OnDelete#SOFT_CASCADE} as those are
 * not protected by the database and therefore orphans might exist.
 *
 * @author Andreas Haufler (aha@scireum.de)
 * @since 2015/05
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

    /**
     * Adds a reference to be checked.
     *
     * @param check the actual check to be performed
     */
    public void addReferenceToCheck(Runnable check) {
        synchronized (referencesToCheck) {
            referencesToCheck.add(check);
            referencesIter = null;
        }
    }
}
