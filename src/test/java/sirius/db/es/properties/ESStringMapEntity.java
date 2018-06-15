/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties;

import sirius.db.es.ElasticEntity;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.types.StringMap;

public class ESStringMapEntity extends ElasticEntity {

    public static final Mapping MAP = Mapping.named("map");
    private final StringMap map = new StringMap();

    public StringMap getMap() {
        return map;
    }
}
