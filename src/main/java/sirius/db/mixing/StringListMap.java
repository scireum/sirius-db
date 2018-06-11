/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StringListMap extends SafeMap<String, List<String>> {

    public List<String> getList(String key) {
        return get(key).orElse(Collections.emptyList());
    }

    public StringListMap add(String key, String value) {
        modify().computeIfAbsent(key, k -> new ArrayList<>()).add(value);
        return this;
    }

    public StringListMap addToSet(String key, String value) {
        List<String> values = modify().computeIfAbsent(key, k -> new ArrayList<>());
        if (!values.contains(value)) {
            values.add(value);
        }

        return this;
    }

    public StringListMap remove(String key, String value) {
        get(key).ifPresent(list -> list.remove(value));
        return this;
    }

    public boolean contains(String key, String value) {
        return get(key).map(list -> list.contains(value)).orElse(false);
    }

    public StringListMap clear(String key) {
        get(key).ifPresent(List::clear);
        return this;
    }

    @Override
    protected boolean valueNeedsCopy() {
        return true;
    }

    @Override
    protected List<String> copyValue(List<String> value) {
        return new ArrayList<>(value);
    }
}
