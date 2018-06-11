/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

public abstract class SafeList<T> implements Iterable<T> {

    private List<T> data;

    public List<T> data() {
        if (data == null) {
            return Collections.emptyList();
        } else {
            return Collections.unmodifiableList(data);
        }
    }

    public List<T> modify() {
        if (data == null) {
            data = new ArrayList<>();
        }

        return data;
    }

    protected abstract boolean valueNeedsCopy();

    protected abstract T copyValue(T value);

    public List<T> original() {
        if (data == null) {
            return Collections.emptyList();
        }

        return data;
    }

    public void setData(List<T> newData) {
        this.data = newData;
    }

    public SafeList<T> add(T item) {
        modify().add(item);
        return this;
    }

    public SafeList<T> clear() {
        if (data != null) {
            data.clear();
        }

        return this;
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

    public boolean contains(T item) {
        return data().contains(item);
    }

    @NotNull
    @Override
    public Iterator<T> iterator() {
        return data().iterator();
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        data().forEach(action);
    }

    @Override
    public Spliterator<T> spliterator() {
        return data().spliterator();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (!(obj instanceof SafeList)) {
            return false;
        }

        return data().equals(((SafeList<?>) obj).data());
    }

    @Override
    public int hashCode() {
        return data().hashCode();
    }

    @Override
    public String toString() {
        return data().toString();
    }

    public List<T> copy() {
        if (data == null) {
            return Collections.emptyList();
        }

        return new ArrayList<>(data);
    }
}
