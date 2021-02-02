/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties;

import sirius.db.mixing.Composite;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.types.MultiLanguageString;

/**
 * Represents a composite to test properties of type {@link MultiLanguageString}
 */
public class MongoMultiLanguageStringComposite extends Composite {
    public static final Mapping COMPOSITE_MULTILANGTEXT_WITH_VALID_LANGUAGES =
            Mapping.named("compositeMultiLangTextWithValidLanguages");
    @NullAllowed
    private final MultiLanguageString compositeMultiLangTextWithValidLanguages =
            new MultiLanguageString().withValidLanguages(MongoMultiLanguageStringEntity.validLanguages);

    public MultiLanguageString getCompositeMultiLangTextWithValidLanguages() {
        return compositeMultiLangTextWithValidLanguages;
    }
}
