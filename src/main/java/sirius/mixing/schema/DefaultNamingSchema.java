/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.schema;

import sirius.kernel.di.std.Register;

import javax.annotation.Nonnull;

/**
 * Created by aha on 03.12.14.
 */
@Register
public class DefaultNamingSchema implements NamingSchema {

    @Override
    public String generateColumnName(Property property) {
        return "f_" ;//+ property.getName();
    }

    @Override
    public String generateTableName(Class<?> type) {
        return "t_" + type.getSimpleName().toLowerCase();
    }

    @Nonnull
    @Override
    public String getName() {
        return "default";
    }
}
