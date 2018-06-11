/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.StringList;
import sirius.db.mongo.MongoEntity;

public class MongoStringListEntity extends MongoEntity {

    public static final Mapping LIST = Mapping.named("list");
    private final StringList list = new StringList();

    public StringList getList() {
        return list;
    }
}
