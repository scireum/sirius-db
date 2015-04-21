/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing;

import sirius.kernel.di.std.Named;
import sirius.mixing.properties.Property;

/**
 * Created by aha on 03.12.14.
 */
public interface NamingSchema extends Named {

    String generateColumnName(String propertyName);

    String generateTableName(Class<?> type);

}
