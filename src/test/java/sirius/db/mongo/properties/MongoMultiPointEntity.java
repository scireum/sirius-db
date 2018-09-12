/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties;

import sirius.db.mixing.Mapping;
import sirius.db.mongo.MongoEntity;
import sirius.db.mongo.types.MultiPointLocation;

public class MongoMultiPointEntity extends MongoEntity {
    public static final Mapping LOCATIONS = Mapping.named("locations");
    private final MultiPointLocation locations = new MultiPointLocation();

    public MultiPointLocation getLocations() {
        return locations;
    }
}
