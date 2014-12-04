/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.schema;

import sirius.kernel.di.std.Named;

/**
 * Created by aha on 03.12.14.
 */
public interface NamingSchema extends Named {

    String generateColumnName(Property property);

    String generateTableName(Class<?> type);

}
