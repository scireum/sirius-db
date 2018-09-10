/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.types.StringIntMap;
import sirius.db.mongo.MongoEntity;

public class MongoStringIntMapEntity extends MongoEntity {
    public static final Mapping MAP = Mapping.named("map");
    private final StringIntMap map = new StringIntMap();

    public StringIntMap getMap() {
        return map;
    }
}
