/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.Index;

@Index(name = "prefix", columns = "prefix", columnSettings = Mango.INDEX_ASCENDING)
@Index(name = "text", columns = "prefix", columnSettings = Mango.INDEX_AS_FULLTEXT)
public class PrefixTestEntity extends MongoEntity {

    public static final Mapping PREFIX = Mapping.named("prefix");
    private String prefix;

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
}
