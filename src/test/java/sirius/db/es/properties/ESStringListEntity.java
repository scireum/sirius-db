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
import sirius.db.mixing.StringList;

public class ESStringListEntity extends ElasticEntity {

    public static final Mapping LIST = Mapping.named("list");
    private final StringList list = new StringList();

    public StringList getList() {
        return list;
    }
}
