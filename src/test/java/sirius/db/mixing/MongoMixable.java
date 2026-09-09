/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.mixing.annotations.Mixin;

@Mixin(RefMongoEntity.class)
public class MongoMixable extends Mixable {

    public static final Mapping COMPOSITE = Mapping.named("composite");
    private final MongoComposite composite = new MongoComposite();

    public MongoComposite getComposite() {
        return composite;
    }
}
