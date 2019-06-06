/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.Length;
import sirius.db.mixing.types.StringList;

public class SQLStringListPropertyEntity extends SQLEntity {

    public static final Mapping STRING_LIST = Mapping.named("stringList");
    @Length(4096)
    private final StringList stringList = new StringList();

    public static final Mapping SHORT_STRING_LIST = Mapping.named("shortStringList");
    @Length(20)
    private final StringList shortStringList = new StringList();

    public StringList getStringList() {
        return stringList;
    }

    public StringList getShortStringList() {
        return shortStringList;
    }
}
