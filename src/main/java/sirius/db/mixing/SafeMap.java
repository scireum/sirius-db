/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;

public abstract class SafeMap<K, V> implements Iterable<Map.Entry<K, V>> {

    protected Map<K, V> data;

    public Map<K, V> data() {
        if (data == null) {
            return Collections.emptyMap();
        } else {
            return Collections.unmodifiableMap(data);
        }
    }

    public Map<K, V> modify() {
        if (data == null) {
            data = new LinkedHashMap<>();
        }

        return data;
    }

    public SafeMap<K, V> put(K key, V value) {
        modify().put(key, value);
        return this;
    }

    public Optional<V> get(K key) {
        return Optional.ofNullable(data().get(key));
    }

    public SafeMap<K, V> clear() {
        if (data != null) {
            data.clear();
        }

        return this;
    }

    public Map<K, V> copyValue() {
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

    protected abstract boolean valueNeedsCopy();

    protected abstract V copyValue(V value);

    public Map<K, V> original() {
        if (data == null) {
            return Collections.emptyMap();
        }

        return data;
    }

    public void setData(Map<K, V> newData) {
        this.data = newData;
    }

    public int size() {
        return data().size();
    }

    public boolean isEmpty() {
        return data().isEmpty();
    }

    public boolean isFilled() {
        return !data().isEmpty();
    }

    public boolean containKey(K key) {
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

    @NotNull
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
