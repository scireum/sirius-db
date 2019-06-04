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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Represents a facet which aggregates on a set of date ranges.
 * <p>
 * Note that a single aggregation in $facet is generated per date range as these might overlap or be non-continuous.
 */
public class MongoDateRangeFacet extends MongoFacet {

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
        int index = 0;
        for (Tuple<DateRange, Integer> rangeAndCounter : ranges) {
            DateRange range = rangeAndCounter.getFirst();
            BasicDBList facet = new BasicDBList();
            String fieldName = descriptor.findProperty(field.toString()).getPropertyName();

            List<Object> boundaries = Arrays.asList(QueryBuilder.FILTERS.transform(range.getFrom()),
                                                    QueryBuilder.FILTERS.transform(range.getUntil().plusSeconds(1)));
            BasicDBObject bucket = new BasicDBObject("groupBy", "$" + fieldName).append("boundaries", boundaries)
                                                                                .append("default",
                                                                                        QueryBuilder.FILTERS.transform(
                                                                                                range.getUntil()
                                                                                                     .plusSeconds(2)));

            facet.add(new BasicDBObject().append("$bucket", bucket));

            facetConsumer.accept(name + index, facet);
            index++;
        }
    }

    @Override
    public void digest(Doc result) {
        int index = 0;
        for (Tuple<DateRange, Integer> range : ranges) {
            Document document = (Document) result.getList(name + index).stream().findFirst().orElse(null);
            if (document != null) {
                Doc doc = new Doc(document);
                range.setSecond(doc.get("count").asInt(0));
            }
            index++;
        }

        if (completionCallback != null) {
            completionCallback.accept(this);
        }
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
