/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import com.google.common.collect.Lists;
import sirius.kernel.async.BackgroundLoop;
import sirius.kernel.async.TaskContext;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Constantly checks if there are unsatisfied foreign key constraints and removes the entities accordingly.
 * <p>
 * An {@link EntityRef} which has {@link EntityRef.OnDelete#LAZY_CASCADE} as delete handler does not
 * instantly remove the entity if its parent is deleted. Rather this background handler eventually picks it up and
 * deletes it. We also check references of type {@link EntityRef.OnDelete#SOFT_CASCADE} as those are
 * not protected by the database and therefore orphans might exist.
 */
@Register(classes = {CascadeDeleteTaskQueue.class, BackgroundLoop.class})
public class CascadeDeleteTaskQueue extends BackgroundLoop {

    private final List<Runnable> referencesToCheck = Lists.newArrayList();

    /**
     * Adds a reference to be checked.
     *
     * @param check the actual check to be performed
     */
    public void addReferenceToCheck(Runnable check) {
        synchronized (referencesToCheck) {
            referencesToCheck.add(check);
        }
    }

    @Nonnull
    @Override
    public String getName() {
        return "DB - Cascade Deletes";
    }

    @Override
    protected void doWork() throws Exception {
        for (Runnable task : referencesToCheck) {
            if (TaskContext.get().isActive()) {
                try {
                    task.run();
                } catch (Exception e) {
                    Exceptions.handle(OMA.LOG, e);
                }
            }
        }
    }
}
