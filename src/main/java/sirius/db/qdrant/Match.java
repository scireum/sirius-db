/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.qdrant;

import com.fasterxml.jackson.databind.node.ObjectNode;
import sirius.kernel.commons.Json;

/**
 * Represents a single match returned by a {@link Search}.
 */
public class Match {

    private final String id;
    private final ObjectNode payload;

    private final float score;

    /**
     * Creates a new match from the given JSON response
     *
     * @param match the match as returned by qdrant
     */
    public Match(ObjectNode match) {
        this.id = match.get("id").asText();
        this.score = match.get("score").floatValue();
        this.payload = match.withObject("/payload");
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
        return fieldType.cast(Json.convertToJavaObject(payload.get(field)));
    }

    public float getScore() {
        return score;
    }
}
