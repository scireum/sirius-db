/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import sirius.kernel.Lifecycle;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;

/**
 * Starts and stops the {@link Mongo} client.
 */
@Register(classes = Lifecycle.class)
public class MongoLifecycle implements Lifecycle {

    @Part
    private Mongo mongo;

    @Override
    public int getPriority() {
        return 75;
    }

    @Override
    public void started() {
        // nothing to do here, MongoDB gets intialized on first usage
    }

    @Override
    public void stopped() {
        // nothing to do here
    }

    @Override
    public void awaitTermination() {
        // We wait until this last call before we cut the connection to the database to permit
        // other stopping lifecycles access until the very end...
        mongo.close();
    }

    @Override
    public String getName() {
        return "MongoDB";
    }
}
