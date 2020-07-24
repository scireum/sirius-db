/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.mixing.types.BaseEntityRef;
import sirius.kernel.cache.Cache;
import sirius.kernel.cache.CacheManager;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.Exceptions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Provides a global cache for field values.
 * <p>
 * This can be used to quickly resolve IDs into names / label when rendering tables of items.
 * Note that the cache isn't invalidated automatically but rather short lived.
 */
@Register(classes = FieldLookupCache.class)
public class FieldLookupCache {

    private Cache<String, Value> cache = CacheManager.createLocalCache("mixing-field-lookup");

    @Part
    private Mixing mixing;

    private <E extends BaseEntity<?>> Value load(Class<E> type, @Nullable Object id, Mapping field) throws Exception {
        return mixing.getDescriptor(type).getMapper().fetchField(type, id, field);
    }

    /**
     * Provides the value of the given entity and field.
     * <p>
     * Note that this gracefully handles both, empty IDs as well as IDs of nonexistent entities by simply returning
     * an empty value.
     *
     * @param type  the type of the entity to resolve
     * @param id    the id of the entity to resolve
     * @param field the field to resolve
     * @param <E>   the generic type of the entitiy
     * @return the value of the field or an empty value if either the field is empty or the given ID was <tt>null</tt>
     */
    public <E extends BaseEntity<?>> Value lookup(Class<E> type, Object id, Mapping field) {
        if (Strings.isEmpty(id)) {
            return null;
        }

        try {
            String cacheKey = getCacheKey(type, id, field);
            Value result = cache.get(cacheKey);
            if (result == null) {
                result = load(type, id, field);
                cache.put(cacheKey, result);
            }

            return result;
        } catch (Exception e) {
            Exceptions.handle()
                      .to(Mixing.LOG)
                      .error(e)
                      .withSystemErrorMessage(
                              "An error occurred when performing a lookup on field %s for entity %s of type %s: %s (%s)",
                              field,
                              id,
                              type)
                      .handle();
            return null;
        }
    }

    @Nonnull
    private <E extends BaseEntity<?>> String getCacheKey(Class<E> type, Object id, Mapping field) {
        return Mixing.getUniqueName(type, id) + "-" + field;
    }

    /**
     * Provides the value of the given entity and field.
     *
     * @param type  the type of the entity to resolve
     * @param id    the id of the entity to resolve
     * @param field the field to resolve
     * @param <E>   the generic type of the entitiy
     * @return the value of the field or an empty value if either the field is empty or the given ID was <tt>null</tt>
     */
    public <E extends BaseEntity<?>> Value lookup(Class<E> type, Object id, String field) {
        return lookup(type, id, Mapping.named(field));
    }

    /**
     * Provides the value of the field for the entity in the given reference.
     *
     * @param ref   the reference which points to the entity to resolve
     * @param field the field to resolve
     * @param <E>   the generic type of the entitiy
     * @return the value of the field or an empty value if either the field is empty or if the given reference was empty
     */
    public <E extends BaseEntity<?>> Value lookup(BaseEntityRef<?, E> ref, String field) {
        return lookup(ref.getType(), ref.getId(), field);
    }

    /**
     * Provides the value of the field for the entity in the given reference.
     *
     * @param ref   the reference which points to the entity to resolve
     * @param field the field to resolve
     * @param <E>   the generic type of the entitiy
     * @return the value of the field or an empty value if either the field is empty or if the given reference was empty
     */
    public <E extends BaseEntity<?>> Value lookup(BaseEntityRef<?, E> ref, Mapping field) {
        return lookup(ref.getType(), ref.getId(), field);
    }
}
