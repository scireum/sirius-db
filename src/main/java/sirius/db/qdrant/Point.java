/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.qdrant;

import com.alibaba.fastjson.JSONObject;
import sirius.db.util.Tensors;
import sirius.kernel.commons.Hasher;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a point to be inserted into qdrant.
 */
public class Point {

    private final String id;
    private float[] vector;
    private Map<String, Object> payload;

    /**
     * As qdrant only supports 128bit IDs, we derive one from a given identifier by computing the SHA1 hash.
     *
     * @param identifier the base identifier to derive the 128bit ID from
     * @return a 128bit (32 hex bytes) id usable by qdrant
     */
    public static String deriveId(String identifier) {
        return Hasher.sha1().hash(identifier).toHexString();
    }

    /**
     * Creates a new point with the given vector.
     *
     * @param id     the id of the point. Note that this should be a UUID or at least a string of 32 bytes which only
     *               use hexadecimal characters (0-F). User either {@link UUID} or {@link #deriveId(String)}.
     * @param vector the vector to store. Use {@link Tensors} to decode string or list representations. Note that the
     *               number of dimensions must match the number of dimensions of the index.
     */
    public Point(String id, float[] vector) {
        this.id = id;
        this.vector = vector.clone();
    }

    /**
     * Adds a payload to the point.
     *
     * @param key   the name of the field to store
     * @param value the value to store. Note that this must be supported within JSON and qdrant.
     * @return the point itself for fluent method calls
     */
    public Point withPayload(String key, Object value) {
        if (payload == null) {
            payload = new HashMap<>();
        }
        this.payload.put(key, value);
        return this;
    }

    protected JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("id", id);
        json.put("vector", vector);
        json.put("payload", payload);

        return json;
    }

    @Override
    public String toString() {
        return id;
    }
}
