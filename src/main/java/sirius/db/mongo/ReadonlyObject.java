/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import org.bson.Document;

import javax.annotation.Nullable;
import java.io.Serial;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Represents an empty and readonly document.
 */
class ReadonlyObject extends Document {

    protected static final Document EMPTY_OBJECT = new ReadonlyObject();

    @Serial
    private static final long serialVersionUID = -1079350674050338133L;

    private ReadonlyObject() {
    }

    @Override
    public Document append(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceAll(BiFunction<? super String, ? super Object, ?> function) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Object putIfAbsent(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(String key, Object oldValue, Object newValue) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Object replace(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object computeIfAbsent(String key, Function<? super String, ?> mappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object computeIfPresent(String key, BiFunction<? super String, ? super Object, ?> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object compute(String key, BiFunction<? super String, ? super Object, ?> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object merge(String key, Object value, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object put(String s, Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> keySet() {
        return Collections.emptySet();
    }
}
