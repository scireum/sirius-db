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
import sirius.db.mixing.types.StringList;

@Mixin(MongoStringListMixinEntity.class)
public class MongoStringListMixin extends Mixable {

    public static final Mapping LIST_IN_MIXIN = Mapping.named("listInMixin");
    private final StringList listInMixin = new StringList();

    public StringList getListInMixin() {
        return listInMixin;
    }
}
