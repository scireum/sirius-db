/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.types;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Describes a list of objects which can be stored in an entity as property value.
 *
 * @param <T> the type of objects stored in this list
 */
public abstract class SafeList<T> implements Iterable<T> {

    private List<T> data;

    /**
     * Provides readonly access to the underlying list.
     *
     * @return a readonly version of the underlying list
     */
    public List<T> data() {
        if (data == null) {
            return Collections.emptyList();
        } else {
            return Collections.unmodifiableList(data);
        }
    }

    /**
     * Provides read and write access to the underlying list.
     *
     * @return the underlying list
     */
    public List<T> modify() {
        if (data == null) {
            data = new ArrayList<>();
        }

        return data;
    }

    /**
     * Determines if values in this list must be copied if the list is copied.
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
    protected abstract T copyValue(T value);

    /**
     * Provides access to the list which was origrinally used to supply contents.
     * <p>
     * In contrast to {@link #modify()} this will not create a new list if none is present yet.
     * Therefore the result might be readonly. The is mainly used by the storage engine to
     * re-use internal data structures as much as possible.
     *
     * @return the original list which was loaded from the database or an empty list if none is present
     */
    public List<T> original() {
        if (data == null) {
            return Collections.emptyList();
        }

        return data;
    }

    /**
     * Sets the underyling list to use.
     * <p>
     * As {@link #original()} this should only be used by the storage engine to insert a database specific
     * implementation which can later be re-used.
     *
     * @param newData the new list to use
     */
    public void setData(List<T> newData) {
        this.data = newData;
    }

    /**
     * Adds a new item to the list.
     *
     * @param item the item to add
     * @return the list itself for fluent method calls
     */
    public SafeList<T> add(T item) {
        modify().add(item);
        return this;
    }

    /**
     * Clears the list.
     *
     * @return the list itself for fluent method calls
     */
    public SafeList<T> clear() {
        if (data != null) {
            data.clear();
        }

        return this;
    }

    /**
     * Determines the size of the list.
     *
     * @return the number of items in the list
     */
    public int size() {
        return data().size();
    }

    /**
     * Determines if the list is empty.
     *
     * @return <tt>true</tt> if the list is empty, <tt>false</tt> otherwise
     */
    public boolean isEmpty() {
        return data().isEmpty();
    }

    /**
     * Determines if the list is not empty.
     *
     * @return <tt>true</tt> if the list is not empty, <tt>false</tt> otherwise
     */
    public boolean isFilled() {
        return !data().isEmpty();
    }

    /**
     * Determines if the list contains the given item.
     *
     * @param item the item to check for
     * @return <tt>true</tt> if the list contains the item, <tt>false</tt> otherwise
     */
    public boolean contains(T item) {
        return data().contains(item);
    }

    @Nonnull
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

    /**
     * Creates a copy of the underlying list.
     * <p>
     * This is used by the framework to permit change tracking. If we wouldn't create a copy of the list, the
     * "original" backup and the list in the entity would be the same and therefore no modifications would be
     * recognized.
     *
     * @return a copy of the internally stored list
     */
    public List<T> copyList() {
        if (data == null) {
            return Collections.emptyList();
        }

        if (valueNeedsCopy()) {
            return data.stream().map(this::copyValue).collect(Collectors.toList());
        } else {
            return new ArrayList<>(data);
        }
    }
}
