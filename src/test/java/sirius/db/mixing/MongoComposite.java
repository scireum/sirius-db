/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.mixing.types.StringMap;

public class MongoComposite extends Composite {

    public static final Mapping MAP = Mapping.named("map");
    private final StringMap map = new StringMap();

    public StringMap getMap() {
        return map;
    }
}
