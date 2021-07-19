/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.facets;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.Document;
import sirius.db.mixing.DateRange;
import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mapping;
import sirius.db.mongo.Doc;
import sirius.db.mongo.QueryBuilder;
import sirius.kernel.commons.Tuple;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Represents a facet which aggregates on a set of date ranges.
 * <p>
 * Note that a single aggregation in $facet is generated per date range as these might overlap or be non-continuous.
 */
public class MongoDateRangeFacet extends MongoFacet {

    private static final String DEFAULT_BUCKET_ID = "default-bucket-id";

    private final Mapping field;
    private List<Tuple<DateRange, Integer>> ranges;
    private Consumer<MongoDateRangeFacet> completionCallback;

    /**
     * Generates a facet with the given name, for the given field and list of ranges.
     *
     * @param name   the name of the facet
     * @param field  the field to aggregate on
     * @param ranges the ranges to aggregate on
     */
    public MongoDateRangeFacet(String name, Mapping field, List<DateRange> ranges) {
        super(name);
        this.field = field;
        this.ranges = ranges.stream().map(range -> Tuple.create(range, 0)).collect(Collectors.toList());
    }

    /**
     * Creates a term facet for the given field and list of ranges.
     *
     * @param field  the field to aggregate on
     * @param ranges the ranges to aggregate on
     */
    public MongoDateRangeFacet(Mapping field, List<DateRange> ranges) {
        this(field.toString(), field, ranges);
    }

    /**
     * Specifies the callback to invoke once the facet was been computed completely.
     *
     * @param completionCallback the callback to invoke
     * @return the facet itself for fluent method calls
     */
    public MongoDateRangeFacet onComplete(Consumer<MongoDateRangeFacet> completionCallback) {
        this.completionCallback = completionCallback;
        return this;
    }

    @Override
    public void emitFacets(EntityDescriptor descriptor, BiConsumer<String, DBObject> facetConsumer) {
        AtomicInteger indexCounter = new AtomicInteger();
        for (Tuple<DateRange, Integer> rangeAndCounter : ranges) {
            DateRange range = rangeAndCounter.getFirst();

            BasicDBObject bucketFacet = new BasicDBObject();

            // We group by the given field...
            String fieldName = descriptor.findProperty(field.toString()).getPropertyName();
            bucketFacet.append("groupBy", "$" + fieldName);

            // The boundaries are [min, max) meaning a value has to fullfill: min <= value < max
            List<Object> boundaries = Arrays.asList(QueryBuilder.FILTERS.transform(range.getFrom()),
                                                    QueryBuilder.FILTERS.transform(range.getUntil().plusSeconds(1)));
            bucketFacet.append("boundaries", boundaries);

            // the bucket id for the results not in the given range
            bucketFacet.append("default", DEFAULT_BUCKET_ID);

            // Append each range as $bucket aggregation
            BasicDBList facetAsList = new BasicDBList();
            facetAsList.add(new BasicDBObject("$bucket", bucketFacet));
            facetConsumer.accept(name + indexCounter.getAndIncrement(), facetAsList);
        }
    }

    @Override
    public void digest(Doc result) {
        AtomicInteger indexCounter = new AtomicInteger();
        for (Tuple<DateRange, Integer> range : ranges) {
            Optional<Document> match = getRangeBucket(result.getList(name + indexCounter.getAndIncrement()));
            match.ifPresent(document -> range.setSecond(document.getInteger("count", 0)));
        }

        if (completionCallback != null) {
            completionCallback.accept(this);
        }
    }

    private Optional<Document> getRangeBucket(List<Object> list) {
        if (list == null) {
            return Optional.empty();
        }

        return list.stream()
                   .filter(Document.class::isInstance)
                   .map(Document.class::cast)
                   .filter(document -> !DEFAULT_BUCKET_ID.equals(document.get("_id")))
                   .findFirst();
    }

    /**
     * Returns the list of ranges paired with the number of matches in each range.
     *
     * @return the list of ranges
     */
    public List<Tuple<DateRange, Integer>> getRanges() {
        return Collections.unmodifiableList(ranges);
    }
}
