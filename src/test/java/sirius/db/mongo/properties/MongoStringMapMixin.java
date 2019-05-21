/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.annotations.Mixin;
import sirius.db.mixing.types.StringMap;

@Mixin(MongoStringMapMixinEntity.class)
public class MongoStringMapMixin extends Mixable {

    public static final Mapping MAP_IN_MIXIN = Mapping.named("mapInMixin");
    private final StringMap mapInMixin = new StringMap();

    public StringMap getMapInMixin() {
        return mapInMixin;
    }
}
