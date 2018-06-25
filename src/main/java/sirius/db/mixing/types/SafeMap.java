/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.types;


import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Describes a map of objects which can be stored in an entity as property value.
 *
 * @param <K> the type of keys of this map
 * @param <V> the type of values of this map
 */
public abstract class SafeMap<K, V> implements Iterable<Map.Entry<K, V>> {

    protected Map<K, V> data;

    /**
     * Provides readonly access to the underlying map.
     *
     * @return a readonly version of the underlying map
     */
    public Map<K, V> data() {
        if (data == null) {
            return Collections.emptyMap();
        } else {
            return Collections.unmodifiableMap(data);
        }
    }

    /**
     * Provides read and write access to the underlying map.
     *
     * @return the underlying map
     */
    public Map<K, V> modify() {
        if (data == null) {
            data = new LinkedHashMap<>();
        }

        return data;
    }

    /**
     * Puts the given key and value into the map.
     *
     * @param key   the key used to store the value
     * @param value the value to store
     * @return the map itself for fluent method calls
     */
    public SafeMap<K, V> put(K key, V value) {
        modify().put(key, value);
        return this;
    }

    /**
     * Returns the value stored for the given key.
     *
     * @param key the key used to lookup the value
     * @return the value wrapped as optional or an empty optional if either <tt>null</tt> or nothing was stored
     * for the given key.
     */
    public Optional<V> get(K key) {
        return Optional.ofNullable(data().get(key));
    }

    /**
     * Clears the entire map.
     *
     * @return the map itself for fluent method calls
     */
    public SafeMap<K, V> clear() {
        if (data != null) {
            data.clear();
        }

        return this;
    }

    /**
     * Creates a copy of the underlying map.
     * <p>
     * This is used by the framework to permit change tracking. If we wouldn't create a copy of the map, the
     * "original" backup and the map in the entity would be the same and therefore no modifications would be
     * recognized.
     *
     * @return a copy of the internally stored map
     */
    public Map<K, V> copyMap() {
        if (data == null) {
            return Collections.emptyMap();
        }

        if (!valueNeedsCopy()) {
            return new LinkedHashMap<>(data);
        }

        LinkedHashMap<K, V> result = new LinkedHashMap<>();
        data.forEach((key, value) -> result.put(key, copyValue(value)));
        return result;
    }

    /**
     * Determines if values in this map must be copied if the map is copied.
     *
     * @return <tt>true</tt> if a copy is required, <tt>false</tt> otherwise
     */
    protected abstract boolean valueNeedsCopy();

    /**
     * Creates a copy of the given value.
     *
     * @param value the value to copy
     * @return the copy of the given value
     */
    protected abstract V copyValue(V value);

    /**
     * Provides access to the map which was origrinally used to supply contents.
     * <p>
     * In contrast to {@link #modify()} this will not create a new map if none is present yet.
     * Therefore the result might be readonly. The is mainly used by the storage engine to
     * re-use internal data structures as much as possible.
     *
     * @return the original map which was loaded from the database or an empty map if none is present
     */
    public Map<K, V> original() {
        if (data == null) {
            return Collections.emptyMap();
        }

        return data;
    }

    /**
     * Sets the underyling map to use.
     * <p>
     * As {@link #original()} this should only be used by the storage engine to insert a database specific
     * implementation which can later be re-used.
     *
     * @param newData the new map to use
     */
    public void setData(Map<K, V> newData) {
        this.data = newData;
    }

    /**
     * Returns the number of entries in the map.
     *
     * @return the number of entries in the map
     */
    public int size() {
        return data().size();
    }

    /**
     * Determines if the map is empty.
     *
     * @return <tt>true</tt> if the map is empty, <tt>false</tt> otherwise
     */
    public boolean isEmpty() {
        return data().isEmpty();
    }

    /**
     * Determines if the map is not empty.
     *
     * @return <tt>true</tt> if the map is not empty, <tt>false</tt> otherwise
     */
    public boolean isFilled() {
        return !data().isEmpty();
    }

    /**
     * Determines if the map contains the given key.
     *
     * @param key the key to check for
     * @return <tt>true</tt> if the map contains the key, <tt>false</tt> otherwise
     */
    public boolean containsKey(K key) {
        return data().containsKey(key);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (!(obj instanceof SafeMap)) {
            return false;
        }

        return data().equals(((SafeMap<?, ?>) obj).data());
    }

    @Override
    public int hashCode() {
        return data().hashCode();
    }

    @Nonnull
    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        return data().entrySet().iterator();
    }

    @Override
    public void forEach(Consumer<? super Map.Entry<K, V>> action) {
        data().entrySet().forEach(action);
    }

    @Override
    public Spliterator<Map.Entry<K, V>> spliterator() {
        return data.entrySet().spliterator();
    }
}
