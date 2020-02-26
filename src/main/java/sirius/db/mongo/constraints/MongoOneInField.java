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
import sirius.db.mixing.Mapping;
import sirius.db.mixing.query.constraints.OneInField;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents an implementation which generates a query optimized for MongoDB.
 */
class MongoOneInField extends OneInField<MongoConstraint> {

    protected MongoOneInField(MongoFilterFactory factory, Mapping field, List<?> values) {
        super(factory, field, values);
    }

    @Override
    public MongoConstraint build() {
        if (values.isEmpty()) {
            if (forceEmpty) {
                return factory.or(factory.notFilled(field), factory.isEmptyArray(field));
            }
            return null;
        }

        if (!orEmpty) {
            BasicDBList list = new BasicDBList();
            for (Object value : values) {
                list.add(factory.transform(value));
            }
            return new MongoConstraint(field.toString(), new Document("$in", list));
        }

        List<MongoConstraint> clauses = new ArrayList<>();
        for (Object value : values) {
            clauses.add(factory.eq(field, value));
        }

        clauses.add(factory.notFilled(field));
        clauses.add(factory.isEmptyArray(field));

        return factory.or(clauses);
    }
}
