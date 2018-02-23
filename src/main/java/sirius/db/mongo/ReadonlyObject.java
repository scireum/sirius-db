/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import com.mongodb.DBObject;
import org.bson.BSONObject;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

class ReadonlyObject implements DBObject {

    static DBObject EMPTY_OBJECT = new ReadonlyObject();

    private ReadonlyObject() {

    }

    @Override
    public Object put(String s, Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(BSONObject bsonObject) {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void putAll(Map map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(String s) {
        return null;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Map toMap() {
        return Collections.emptyMap();
    }

    @Override
    public Object removeField(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(String s) {
        return false;
    }

    @Override
    public boolean containsField(String s) {
        return false;
    }

    @Override
    public Set<String> keySet() {
        return Collections.emptySet();
    }

    @Override
    public void markAsPartialObject() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isPartialObject() {
        return false;
    }
}
