/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.schema;

import sirius.kernel.di.std.Register;
import sirius.mixing.Entity;

@Register(classes = Schema.class)
public class Schema {
    public EntityDescriptor getDescriptor(Class<? extends Entity> aClass) {
        return null;
    }
}
