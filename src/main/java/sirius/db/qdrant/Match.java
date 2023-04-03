/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.qdrant;

import com.alibaba.fastjson.JSONObject;

/**
 * Represents a single match returned by a {@link Search}.
 */
public class Match {

    private final String id;
    private final JSONObject payload;

    private final float score;

    /**
     * Creates a new match with the given id, score and payload.
     *
     * @param id      the id of the matched point
     * @param score   the score or distance of the match
     * @param payload the payload of the matched point
     */
    public Match(String id, float score, JSONObject payload) {
        this.id = id;
        this.score = score;
        this.payload = payload;
    }

    public String getId() {
        return id;
    }

    /**
     * Extracts the requested payload field and casts it to the given type.
     *
     * @param fieldType the expected field type
     * @param field     the field to load
     * @param <T>       the expected field type
     * @return the value of the field or <tt>null</tt> if the field is not present
     */
    public <T> T getPayload(Class<T> fieldType, String field) {
        return fieldType.cast(payload.get(field));
    }

    public float getScore() {
        return score;
    }
}
