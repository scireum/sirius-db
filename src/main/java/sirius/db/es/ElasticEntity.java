/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import sirius.db.mixing.BaseEntity;
import sirius.db.mixing.BaseMapper;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.annotations.Transient;
import sirius.db.mixing.query.Query;
import sirius.db.mixing.query.constraints.Constraint;
import sirius.kernel.commons.Explain;
import sirius.kernel.di.std.Part;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents the base class for all entities which are managed via {@link Elastic} and stored in Elasticsearch.
 * <p>
 * If possible, it is highly recommended to mark a field to use as routing for this entity to increase performance
 * noticeably. This is done by annotating the field with {@link sirius.db.es.annotations.RoutedBy}.
 * <p>
 * For more info on why its a good idea to use custom routing, visit:
 * https://www.elastic.co/blog/customizing-your-document-routing.
 */
public abstract class ElasticEntity extends BaseEntity<String> {

    private static final String MATCHED_QUERIES = "matched_queries";
    private static final String FIELD_SCORE = "_score";

    @Part
    protected static Elastic elastic;

    /**
     * Contains the {@link #ID} which is auto-generated when inserting a new entity into Elasticsearch.
     * <p>
     * It is {@link NullAllowed} as it is filled during the update but after the save checkes have completed.
     */
    @NullAllowed
    private String id;

    @Transient
    protected long primaryTerm = 0;

    @Transient
    protected long seqNo = 0;

    @Transient
    private JSONObject searchHit;

    @Transient
    private Set<String> matchedQueries;

    @Override
    public boolean isUnique(Mapping field, Object value, Mapping... within) {
        ElasticQuery<? extends ElasticEntity> qry = elastic.select(getClass()).eq(field, value);
        for (Mapping withinField : within) {
            qry.eq(withinField, getDescriptor().getProperty(withinField).getValue(this));
        }
        if (!isNew()) {
            qry.ne(ID, getId());
        }
        return !qry.exists();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E extends BaseEntity<?>, C extends Constraint, Q extends Query<Q, E, C>> BaseMapper<E, C, Q> getMapper() {
        return (BaseMapper<E, C, Q>) elastic;
    }

    @Override
    public String getId() {
        return id;
    }

    /**
     * Note that only the framework must use this to specify the ID of the entity.
     *
     * @param id the id of this entity
     */
    protected void setId(String id) {
        this.id = id;
    }

    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Explain("We only pass the result JSON along internally and want to avoid an extra copy.")
    protected void setSearchHit(JSONObject searchHit) {
        this.searchHit = searchHit;
    }

    /**
     * Gets the list of all named queries which matched this entity.
     * <p>
     * Note: This will be only populated, if the source of the entity is a query.
     *
     * @return the list of named queries which matched this entity.
     */
    public Set<String> getMatchedQueries() {
        if (matchedQueries == null) {
            matchedQueries = parseMatchedQueries();
        }

        return Collections.unmodifiableSet(matchedQueries);
    }

    private Set<String> parseMatchedQueries() {
        if (searchHit == null) {
            return Collections.emptySet();
        }

        JSONArray matchedQueriesArray = searchHit.getJSONArray(MATCHED_QUERIES);
        if (matchedQueriesArray == null) {
            return Collections.emptySet();
        }

        return matchedQueriesArray.stream().filter(Objects::nonNull).map(String::valueOf).collect(Collectors.toSet());
    }

    /**
     * Returns the score (match quality) as computed by Elasticsearch.
     *
     * @return the score assigned to the underlying hit of this entity
     */
    public float getScore() {
        if (searchHit == null) {
            return 0f;
        }

        return searchHit.getFloatValue(FIELD_SCORE);
    }

    /**
     * Checks wether the query with the given name matched this entity.
     *
     * @param queryName the name of the query to check
     * @return the list of named queries which matched this entity.
     */
    public boolean isMatchedNamedQuery(String queryName) {
        return getMatchedQueries().contains(queryName);
    }

    /**
     * Provides access to the original JSON hit which was returned by an Elasticsearch query.
     *
     * @return the original underlying hit object of this entity.
     */
    @Nullable
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Explain("Performing a deep copy of the whole object is most probably an overkill here.")
    public JSONObject getSearchHit() {
        return searchHit;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    protected void setPrimaryTerm(long primaryTerm) {
        this.primaryTerm = primaryTerm;
    }

    public long getSeqNo() {
        return seqNo;
    }

    protected void setSeqNo(long seqNo) {
        this.seqNo = seqNo;
    }
}
