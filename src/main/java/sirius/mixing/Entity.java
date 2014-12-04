/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing;

import sirius.kernel.di.morphium.Adaptable;
import sirius.kernel.di.std.Part;
import sirius.mixing.schema.EntityDescriptor;
import sirius.mixing.schema.Schema;

/**
 * Created by aha on 29.11.14.
 */
public class Entity<I> implements Adaptable {

    @Part
    private static Schema schema;

    public EntityDescriptor getDescriptor() {
        return schema.getDescriptor(getClass());
    }

    private I id;

    public boolean isNew() {
        return id == null;
    }

    public I getId() {
        return id;
    }

    public void setId(I id) {
        this.id = id;
    }
}
