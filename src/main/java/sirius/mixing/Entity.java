/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.mixing;

import com.google.common.collect.Maps;
import sirius.kernel.di.morphium.Adaptable;
import sirius.kernel.di.std.Part;
import sirius.mixing.annotations.Transient;

import java.util.Map;

/**
 * Created by aha on 29.11.14.
 */
public abstract class Entity extends Mixable {

    @Part
    private static Schema schema;

    @Transient
    protected long id = -1;

    @Transient
    protected int version = 0;

    @Transient
    protected Map<String, Object> persistedData = Maps.newHashMap();

    @Transient
    protected Map<Class<?>, Object> mixins;

    public EntityDescriptor getDescriptor() {
        return schema.getDescriptor(getClass());
    }

    protected EntityDescriptor createDescriptor() {
        return new EntityDescriptor(getClass());
    }

    public boolean isNew() {
        return id < 0;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }


    @Override
    public int hashCode() {
        if (id < 0) {
            // Return a hash code appropriate to the implementation of equals.
            return super.hashCode();
        }
        return (int) (id % Integer.MAX_VALUE);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (!(other instanceof Entity)) {
            return false;
        }
        if (isNew()) {
            if (((Entity) other).isNew()) {
                return super.equals(other);
            } else {
                return false;
            }
        }

        return id == ((Entity) other).id;
    }


}
