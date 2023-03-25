/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.types;

import sirius.db.es.NearestNeighborsSearch;
import sirius.kernel.commons.Strings;

import javax.annotation.Nonnull;

/**
 * Specifies a dense vector to be stored in {@linkplain sirius.db.es.ElasticEntity elastic entities}.
 * <p>
 * Such fields are essentially vectors of floats and usually filled by performing an embedding of a text field or
 * image data. Based on the fact that "similar" values yield similar vectors, this can be used to perform nearest
 * neighbor searches during retrieval. Using {@link sirius.db.es.ElasticQuery#knn(NearestNeighborsSearch)} an
 * approximate nearest neighbor search can be performed which is implemented using rather efficient HNSW graphs.
 */
public class DenseVector {

    /**
     * Specified the similarity function to apply.
     */
    public enum Similarity {
        COSINE("cosine"), EUCLIDEAN("l2_norm"), INNER_PRODUCT("dot_product");

        private final String esName;

        Similarity(String esName) {
            this.esName = esName;
        }

        public String getEsName() {
            return esName;
        }
    }

    protected final int dimensions;
    protected final Similarity similarity;
    protected final boolean indexed;

    protected float[] vector;

    /**
     * Creates a new vector to be stored in an entity.
     *
     * @param dimenstions the number of dimensions of the vector
     * @param similarity  the similarity function to apply
     * @param indexed     <tt>true</tt> if the vector should be indexed, <tt>false</tt> otherwise. Note that efficient
     *                    searches need an indexed vector as otherwise the whole index needs to be scanned.
     */
    public DenseVector(int dimenstions, Similarity similarity, boolean indexed) {
        this.dimensions = dimenstions;
        this.similarity = similarity;
        this.indexed = indexed;
        this.vector = new float[dimensions];
    }

    /**
     * Stores actual values in the vector field.
     * <p>
     * Note that the dimensionality must match the one specified when creating the vector.
     *
     * @param vector the data to store
     */
    public void storeVector(@Nonnull Object[] vector) {
        if (vector.length != dimensions) {
            throw new IllegalArgumentException(Strings.apply("Vector has wrong dimensions (given: %s, expected: %s)",
                                                             vector.length,
                                                             dimensions));
        }

        for (int i = 0; i < dimensions; i++) {
            this.vector[i] = ((Number) vector[i]).floatValue();
        }
    }

    /**
     * Retrieves the stored vector.
     *
     * @return the stored vector data
     */
    public float[] loadVector() {
        return vector.clone();
    }
}
