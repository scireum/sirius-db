/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.alibaba.fastjson.JSONObject;
import sirius.db.es.constraints.BoolQueryBuilder;
import sirius.db.es.constraints.ElasticConstraint;
import sirius.db.mixing.Mapping;

import javax.annotation.Nonnull;

/**
 * Describes a kNN "Nearest Neighbors" search.
 * <p>
 * Although named as such in the Elastic documentation, this is not a true kNN search. Instead, it is an approximate
 * nearest neightbor (ANN) search based on hierarchical navigable small world graphs (HNSW).
 *
 * @see ElasticQuery#knn(NearestNeighborsSearch)
 */
public class NearestNeighborsSearch {

    private Mapping field;

    private BoolQueryBuilder filterBuilder;

    private int numResults;

    private int numCandidates;

    private float[] queryVector;

    /**
     * Specifies how to set up the ANN search.
     *
     * @param field         the field to search in (must be of type dense_vector)
     * @param queryVector   the vector to search for
     * @param numResults    the number of top results to return
     * @param numCandidates the number of candidates to consider (must be &gt;= numResults)
     */
    public NearestNeighborsSearch(@Nonnull Mapping field,
                                  @Nonnull float[] queryVector,
                                  int numResults,
                                  int numCandidates) {
        this.field = field;
        this.numResults = numResults;
        this.numCandidates = numCandidates;
        this.queryVector = queryVector.clone();
    }

    /**
     * Adds a filter to the search.
     *
     * @param filter the filter constraint to apply
     * @return the search itself for fluent method calls
     */
    public NearestNeighborsSearch filter(JSONObject filter) {
        if (filter != null) {
            if (filterBuilder == null) {
                filterBuilder = new BoolQueryBuilder();
            }
            filterBuilder.filter(filter);
        }
        return this;
    }

    /**
     * Adds a constraint to filter the search.
     *
     * @param constraint the constraint to apply
     * @return the search itself for fluent method calls
     */
    public NearestNeighborsSearch filter(ElasticConstraint constraint) {
        if (constraint != null) {
            return filter(constraint.toJSON());
        }

        return this;
    }

    protected NearestNeighborsSearch copy() {
        NearestNeighborsSearch copy = new NearestNeighborsSearch(field, queryVector, numResults, numCandidates);
        if (filterBuilder != null) {
            copy.filterBuilder = filterBuilder.copy();
        }

        return copy;
    }

    protected JSONObject build() {
        JSONObject result = new JSONObject();
        result.put("field", field.getName());
        result.put("k", numResults);
        result.put("num_candidates", numCandidates);
        if (filterBuilder != null) {
            result.put("filter", filterBuilder.build());
        }
        result.put("query_vector", queryVector);

        return result;
    }
}
