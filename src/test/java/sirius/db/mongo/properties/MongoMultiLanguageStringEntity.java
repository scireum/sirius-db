/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.types.MultiLanguageString;
import sirius.db.mongo.MongoEntity;

import java.util.Arrays;
import java.util.List;

/**
 * Represents an entity to test properties of type {@link MultiLanguageString}
 */
public class MongoMultiLanguageStringEntity extends MongoEntity {
    public static final Mapping MULTILANGTEXT = Mapping.named("multiLangText");
    @NullAllowed
    private final MultiLanguageString multiLangText = new MultiLanguageString();

    public static final Mapping MULTILANGTEXT_WITH_FALLBACK = Mapping.named("multiLangTextWithFallback");
    @NullAllowed
    private final MultiLanguageString multiLangTextWithFallback = new MultiLanguageString().withFallback();

    public static final List<String> validLanguages =
            Arrays.asList("da", "nl", "en", "fi", "fr", "de", "hu", "it", "nb", "pt", "ro", "ru", "es", "sv", "tr");
    public static final Mapping MULTILANGTEXT_WITH_VALID_LANGUAGES = Mapping.named("multiLangTextWithValidLanguages");
    @NullAllowed
    private final MultiLanguageString multiLangTextWithValidLanguages =
            new MultiLanguageString().withValidLanguages(validLanguages);

    private final MongoMultiLanguageStringComposite multiLangComposite = new MongoMultiLanguageStringComposite();

    public MultiLanguageString getMultiLangText() {
        return multiLangText;
    }

    public MultiLanguageString getMultiLangTextWithFallback() {
        return multiLangTextWithFallback;
    }

    public MultiLanguageString getMultiLangTextWithValidLanguages() {
        return multiLangTextWithValidLanguages;
    }

    public MongoMultiLanguageStringComposite getMultiLangComposite() {
        return multiLangComposite;
    }
}
