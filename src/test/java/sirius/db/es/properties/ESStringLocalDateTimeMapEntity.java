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
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.types.StringLocalDateTimeMap;

public class ESStringLocalDateTimeMapEntity extends ElasticEntity {

    public static final Mapping MAP = Mapping.named("map");
    @NullAllowed
    private final StringLocalDateTimeMap map = new StringLocalDateTimeMap();

    public StringLocalDateTimeMap getMap() {
        return map;
    }
}
