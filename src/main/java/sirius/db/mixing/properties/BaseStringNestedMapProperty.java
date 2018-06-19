/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties;

import sirius.db.mixing.AccessPath;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.Mixing;
import sirius.db.mixing.types.StringNestedMap;
import sirius.kernel.di.std.Part;

import java.lang.reflect.Field;

/**
 * Provides basic method to represent a {@link StringNestedMap} field within a {@link Mixable}.
 */
public class BaseStringNestedMapProperty extends BaseMapProperty {

    @Part
    private static Mixing mixing;
    private EntityDescriptor nestedDescriptor;

    protected BaseStringNestedMapProperty(EntityDescriptor descriptor, AccessPath accessPath, Field field) {
        super(descriptor, accessPath, field);
    }

    protected EntityDescriptor getNestedDescriptor() {
        if (nestedDescriptor == null) {
            nestedDescriptor =
                    mixing.getDescriptor(((StringNestedMap<?>) getMap(descriptor.getReferenceInstance())).getNestedType());
        }

        return nestedDescriptor;
    }
}
