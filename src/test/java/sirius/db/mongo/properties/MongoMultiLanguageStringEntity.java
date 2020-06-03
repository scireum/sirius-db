/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.types.MultiLanguageString;
import sirius.db.mongo.MongoEntity;

/**
 * Represents an entity to test properties of type {@link MultiLanguageString}
 */
public class MongoMultiLanguageStringEntity extends MongoEntity {
    public static final Mapping MULTILANGTEXT = Mapping.named("multiLangText");
    private final MultiLanguageString multiLangText = new MultiLanguageString();

    public static final Mapping MULTILANGTEXT_WITH_FALLBACK = Mapping.named("multiLangTextWithFallback");
    private final MultiLanguageString multiLangTextWithFallback = new MultiLanguageString(true);

    public MultiLanguageString getMultiLangText() {
        return multiLangText;
    }

    public MultiLanguageString getMultiLangTextWithFallback() {
        return multiLangTextWithFallback;
    }
}
