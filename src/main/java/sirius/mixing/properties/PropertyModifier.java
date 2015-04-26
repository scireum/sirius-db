/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing.properties;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Created by aha on 20.04.15.
 */
public interface PropertyModifier {

    @Nullable Class<?> targetType();

    @Nullable String targetFieldName();

    @Nonnull Property modify(@Nonnull Property property);

}
