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
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Tuple;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Generates filters and constraints for {@link sirius.db.mongo.MongoQuery}.
 *
 * @see sirius.db.mongo.QueryBuilder#FILTERS
 */
public class MongoFilterFactory extends FilterFactory<MongoConstraint> {

    /**
     * Represents a regular expression which detects all character which aren't allowed in a search prefix
     * for {@link #prefix(Mapping, String)}.
     */
    public static final Pattern NON_PREFIX_CHARACTER = Pattern.compile("[^0-9\\p{L}_\\-@.#]");

    @Override
    protected Object customTransform(Object value) {
        if (value instanceof LocalDate localDate) {
            return Date.from(localDate.atStartOfDay()
                                      .truncatedTo(ChronoUnit.MILLIS)
                                      .atZone(ZoneId.systemDefault())
                                      .toInstant());
        }
        if (value instanceof LocalDateTime localDateTime) {
            return Date.from(localDateTime.truncatedTo(ChronoUnit.MILLIS).atZone(ZoneId.systemDefault()).toInstant());
        }
        if (value instanceof LocalTime localTime) {
            return Date.from(localTime.atDate(LocalDate.now(ZoneId.systemDefault()))
                                      .truncatedTo(ChronoUnit.MILLIS)
                                      .atZone(ZoneId.systemDefault())
                                      .toInstant());
        }
        if (value instanceof Instant instant) {
            return Date.from(instant.truncatedTo(ChronoUnit.MILLIS));
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

    @Override
    public MongoConstraint isEmptyList(Mapping field) {
        return new MongoOneInField(this, field, Arrays.asList(null, new BasicDBList())).build();
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
        if ("$or".equals(constraint.getKey())) {
            return new MongoConstraint("$nor", constraint.getObject());
        }
        if (constraint.getKey().startsWith("$")) {
            throw new IllegalArgumentException(Strings.apply("The %s constraint can't be easily inverted!",
                                                             constraint.getKey()));
        }
        return new MongoConstraint(constraint.getKey(), new Document("$not", constraint.getObject()));
    }

    @Override
    protected MongoConstraint effectiveAnd(List<MongoConstraint> effectiveConstraints) {
        return new MongoConstraint("$and",
                                   effectiveConstraints.stream()
                                                       .map(clause -> new Document(clause.getKey(), clause.getObject()))
                                                       .toList());
    }

    @Override
    protected MongoConstraint effectiveOr(List<MongoConstraint> effectiveConstraints) {
        return new MongoConstraint("$or",
                                   effectiveConstraints.stream()
                                                       .map(clause -> new Document(clause.getKey(), clause.getObject()))
                                                       .toList());
    }

    @Override
    public OneInField<MongoConstraint> oneInField(Mapping field, Collection<?> values) {
        return new MongoOneInField(this, field, values);
    }

    @Override
    public MongoConstraint noneInField(Mapping field, Collection<?> values) {
        BasicDBList list = new BasicDBList();
        for (Object value : values) {
            list.add(transform(value));
        }
        return new MongoConstraint(field.toString(), new Document("$nin", list));
    }

    /**
     * Creates a constraint which ensures that the given field contains all the given values.
     *
     * @param field  the field to filter on
     * @param values the values to check
     * @return the generated constraint
     */
    @Override
    public MongoConstraint allInField(Mapping field, Collection<?> values) {
        BasicDBList list = new BasicDBList();
        for (Object value : values) {
            list.add(transform(value));
        }
        return new MongoConstraint(field.toString(), new Document("$all", list));
    }

    @Override
    public Tuple<MongoConstraint, Boolean> compileString(EntityDescriptor descriptor,
                                                         String query,
                                                         List<QueryField> fields) {
        MongoQueryCompiler compiler = new MongoQueryCompiler(this, descriptor, query, fields);
        return Tuple.create(compiler.compile(), compiler.isDebugging());
    }

    /**
     * Builds a filter which represents a regex filter for the given field and expression.
     *
     * @param key        the name of the field to check
     * @param expression the regular expression to apply
     * @param options    the options to apply like "i" to match case-insensitive
     * @return a filter representing the given operation
     */
    public MongoConstraint regex(Mapping key, Object expression, String options) {
        return new MongoConstraint(key.toString(), new Document("$regex", expression).append("$options", options));
    }

    /**
     * Builds a filter which represents an 'elemMatch' filter for the given array field and query.
     *
     * @param arrayField the array field to query for
     * @param query      the query that at least one value in the array should match
     * @return a filter representing the 'elemMatch' operation
     */
    public MongoConstraint elemMatch(Mapping arrayField, Document query) {
        return new MongoConstraint(arrayField.toString(), new Document("$elemMatch", query));
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
     * Builds a filter which represents a regex filter representing the given prefix filter.
     * <p>
     * Regular expressions of this kind can utilize an index and are therefore fast.
     *
     * @param key    the name of the field to check
     * @param prefix the prefix to scan for
     * @return a filter representing the given operation
     */
    @Nullable
    public MongoConstraint prefix(Mapping key, String prefix) {
        if (Strings.isEmpty(prefix)) {
            return null;
        }

        String escapedPrefix =
                NON_PREFIX_CHARACTER.matcher(prefix).replaceAll("").replace(".", "\\.").replace("-", "\\-");

        return new MongoConstraint(key.toString(), new Document("$regex", "^" + escapedPrefix.toLowerCase()));
    }

    /**
     * Executes a $text search in the underlying fulltext index.
     * <p>
     * If the given value is empty, no constraint will be generated.
     * <p>
     * Due to the nature of the MongoDB implementation on full token matches are successful. To provide a
     * prefix search use {@link #prefix(Mapping, String)} and ensure that a proper index is present.
     *
     * @param value the token to search for
     * @return a filter representing the $text constraint
     */
    @Nullable
    public MongoConstraint text(String value) {
        if (Strings.isEmpty(value)) {
            return null;
        }

        return new MongoConstraint("$text", new Document("$search", value));
    }

    /**
     * Builds a constraint regarding the {@code $size} of a (list) mapping.
     * <p>
     * See <a href="https://docs.mongodb.com/manual/reference/operator/query/size/">the manual</a> for usage information
     * and restrictions concerning the operator.
     *
     * @param mapping the mapping to filter
     * @param value   the target size to compare against
     * @return the generated constraint
     */
    public MongoConstraint hasListSize(Mapping mapping, int value) {
        return new MongoConstraint(mapping.toString(), new Document("$size", value));
    }
}
