/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.constraints;

import com.fasterxml.jackson.databind.node.ObjectNode;
import sirius.db.es.Elastic;
import sirius.db.mixing.query.constraints.Constraint;
import sirius.kernel.commons.Explain;

/**
 * Defines a constraint which is accepted by {@link sirius.db.es.ElasticQuery} and most probably generated by
 * {@link ElasticFilterFactory}.
 *
 * @see sirius.db.es.Elastic#FILTERS
 */
public class ElasticConstraint extends Constraint {

    private ObjectNode constraint;

    /**
     * Creates a new constraint represented as JSON.
     *
     * @param constraint the JSON making up the constraint
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Explain("Deep copy of a JSON object is too expensive here as it is mostly an internal API")
    public ElasticConstraint(ObjectNode constraint) {
        this.constraint = constraint;
    }

    @Override
    public void asString(StringBuilder builder) {
        builder.append(constraint);
    }

    /**
     * Returns the JSON representation of this constraint.
     *
     * @return the JSON to send to the Elasticsearch server for this constraint
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Explain("Deep copy of a JSON object is too expensive here as it is mostly an internal API")
    public ObjectNode toJSON() {
        return constraint;
    }

    /**
     * Wraps the given constraint using {@link ElasticFilterFactory#constantScore(ElasticConstraint, float)}.
     * <p>
     * Note that a constraint which influences the scoring must be added using
     * {@link sirius.db.es.ElasticQuery#must(ElasticConstraint)}.
     *
     * @param boost the boost or constant score to apply to this constraint
     * @return the newly created constraint
     */
    public ElasticConstraint withConstantScore(float boost) {
        return Elastic.FILTERS.constantScore(this, boost);
    }
}
