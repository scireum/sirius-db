/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.types.StringListMap;
import sirius.db.mongo.MongoEntity;

public class MongoStringListMapEntity extends MongoEntity {

    public static final Mapping MAP = Mapping.named("map");
    private final StringListMap map = new StringListMap();

    public StringListMap getMap() {
        return map;
    }
}
