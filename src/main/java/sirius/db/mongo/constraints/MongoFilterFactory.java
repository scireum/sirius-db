/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.constraints;

import com.mongodb.BasicDBList;
import org.bson.Document;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.query.QueryField;
import sirius.db.mixing.query.constraints.FilterFactory;
import sirius.db.mixing.query.constraints.OneInField;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Generates filters and constraints for {@link sirius.db.mongo.MongoQuery}.
 *
 * @see sirius.db.mongo.QueryBuilder#FILTERS
 */
public class MongoFilterFactory extends FilterFactory<MongoConstraint> {

    @Override
    protected Object customTransform(Object value) {
        if (value instanceof LocalDate) {
            return Date.from(((LocalDate) value).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
        }
        if (value instanceof LocalDateTime) {
            return Date.from(((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant());
        }
        if (value instanceof LocalTime) {
            return Date.from(((LocalTime) value).atDate(LocalDate.now(ZoneId.systemDefault()))
                                                .atZone(ZoneId.systemDefault())
                                                .toInstant());
        }
        if (value instanceof Instant) {
            return Date.from((Instant) value);
        }

        return value;
    }

    @Override
    protected MongoConstraint eqValue(Mapping field, Object value) {
        return new MongoConstraint(field.toString(), new Document("$eq", value));
    }

    @Override
    protected MongoConstraint neValue(Mapping field, Object value) {
        return new MongoConstraint(field.toString(), new Document("$ne", value));
    }

    @Override
    protected MongoConstraint gtValue(Mapping field, Object value, boolean orEqual) {
        return new MongoConstraint(field.toString(), new Document(orEqual ? "$gte" : "$gt", value));
    }

    @Override
    protected MongoConstraint ltValue(Mapping field, Object value, boolean orEqual) {
        return new MongoConstraint(field.toString(), new Document(orEqual ? "$lte" : "$lt", value));
    }

    @Override
    public MongoConstraint filled(Mapping field) {
        return neValue(field, null);
    }

    @Override
    public MongoConstraint notFilled(Mapping field) {
        return eqValue(field, null);
    }

    /**
     * Checks whether the given field is present, independent of the field value (specially <tt>null</tt>).
     *
     * @param field the field to filter
     * @return the generated constraint
     */
    public MongoConstraint exists(Mapping field) {
        return new MongoConstraint(field.toString(), new Document("$exists", true));
    }

    /**
     * Checks whether the given field is absent, independent of the field value (specially <tt>null</tt>).
     *
     * @param field the field to filter
     * @return the generated constraint
     */
    public MongoConstraint notExists(Mapping field) {
        return new MongoConstraint(field.toString(), new Document("$exists", false));
    }

    @Override
    protected MongoConstraint invert(MongoConstraint constraint) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected MongoConstraint effectiveAnd(List<MongoConstraint> effectiveConstraints) {
        return new MongoConstraint("$and",
                                   effectiveConstraints.stream()
                                                       .map(clause -> new Document(clause.getKey(), clause.getObject()))
                                                       .collect(Collectors.toList()));
    }

    @Override
    protected MongoConstraint effectiveOr(List<MongoConstraint> effectiveConstraints) {
        return new MongoConstraint("$or",
                                   effectiveConstraints.stream()
                                                       .map(clause -> new Document(clause.getKey(), clause.getObject()))
                                                       .collect(Collectors.toList()));
    }

    @Override
    public OneInField<MongoConstraint> oneInField(Mapping field, List<Object> values) {
        return new MongoOneInField(this, field, values);
    }

    @Override
    public MongoConstraint noneInField(Mapping field, List<Object> values) {
        BasicDBList list = new BasicDBList();
        for (Object value : values) {
            list.add(transform(value));
        }
        return new MongoConstraint("$nin", new Document(field.toString(), list));
    }

    @Override
    public MongoConstraint queryString(EntityDescriptor descriptor, String query, List<QueryField> fields) {
        return new MongoQueryCompiler(this, descriptor, query, fields).compile();
    }

    /**
     * Builds a filter which represents a regex filter for the given field and expression.
     *
     * @param key        the name of the field to check
     * @param expression the regular expression to apply
     * @param options    the options to apply like "i" to match case insensitive
     * @return a filter representing the given operation
     */
    public MongoConstraint regex(Mapping key, Object expression, String options) {
        return new MongoConstraint(key.toString(), new Document("$regex", expression).append("$options", options));
    }

    /**
     * Builds a filter which represents a geospatial query.
     *
     * @param key               the name of the field to check
     * @param geometry          the geometry used to filter by
     * @param maxDistanceMeters the max distance to consider relevant
     * @return a filter representing the given operation
     */
    public MongoConstraint nearSphere(Mapping key, Document geometry, int maxDistanceMeters) {
        return new MongoConstraint(key.toString(),
                                   new Document("$nearSphere",
                                                new Document("$geometry", geometry).append("$maxDistance",
                                                                                           maxDistanceMeters)));
    }

    /**
     * Builds a filter which represents a geospatial query for a point.
     *
     * @param key               the name of the field to check
     * @param lat               the latitude of the point used as search geometry.
     * @param lon               the longitude of the point used as search geometry.
     * @param maxDistanceMeters the max distance to consider relevant
     * @return a filter representing the given operation
     */
    public MongoConstraint nearSphere(Mapping key, double lat, double lon, int maxDistanceMeters) {
        BasicDBList coordinates = new BasicDBList();
        coordinates.add(lat);
        coordinates.add(lon);

        return nearSphere(key, new Document("type", "Point").append("coordinates", coordinates), maxDistanceMeters);
    }

    /**
     * Executes a $text search in the underlying fulltext index.
     *
     * @param value the token to search for
     * @return a filter representing the $text constraint
     */
    public MongoConstraint text(String value) {
        return new MongoConstraint("$text", new Document("$search", value));
    }
}
