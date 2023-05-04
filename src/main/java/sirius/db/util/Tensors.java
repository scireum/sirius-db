/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.util;

import com.fasterxml.jackson.databind.node.ArrayNode;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;

/**
 * Provides some helpers to work with tensors (dense vectors used by AI models).
 */
public class Tensors {

    private Tensors() {
    }

    /**
     * Provides a helper method to savely convert a list of numbers to a float array.
     *
     * @param vector the list to convert
     * @return the converted array
     */
    public static float[] fromList(List<Number> vector) {
        float[] result = new float[vector.size()];
        for (int i = 0; i < vector.size(); i++) {
            result[i] = vector.get(i).floatValue();
        }

        return result;
    }

    /**
     * Savely extracts a float array from a JSON array.
     *
     * @param vector the JSON array to extract the float array from
     * @return the extracted float array
     */
    public static float[] fromJSON(ArrayNode vector) {
        float[] result = new float[vector.size()];
        for (int i = 0; i < vector.size(); i++) {
            result[i] = vector.get(i).isFloat() ? vector.get(i).floatValue() : 0;
        }

        return result;
    }

    /**
     * Parses a base64 encoded byte array back into a float array.
     * <p>
     * This string was previously created using {@link #encode(float[])}.
     *
     * @param vector the string to parse
     * @return the parsed float array
     */
    public static float[] parse(String vector) {
        byte[] data = Base64.getDecoder().decode(vector);
        float[] result = new float[data.length / Float.BYTES];
        ByteBuffer.wrap(data).asFloatBuffer().get(result, 0, result.length);
        return result;
    }

    /**
     * Encodes the given float array as base64 string.
     * <p>
     * This can be used to store a dense vector in a string field (e.g. a regular database or data exchange file).
     *
     * @param vector the vector to encode
     * @return an encoded base64 string of the float array which is directly cast into a byte array.
     */
    public static String encode(float[] vector) {
        ByteBuffer buffer = ByteBuffer.allocate(vector.length * Float.BYTES);
        buffer.asFloatBuffer().put(vector);
        return Base64.getEncoder().encodeToString(buffer.array());
    }
}
