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
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.types.MultiLanguageString;

import java.util.ArrayList;

/**
 * Represents an entity with a mixin to test properties of type {@link MultiLanguageString}
 */
@Mixin(MongoMultiLanguageStringEntityWithMixin.class)
public class MongoMultiLanguageStringMixin extends Mixable {
    public static final Mapping MIXIN_MULTILANGTEXT_WITH_VALID_LANGUAGES =
            Mapping.named("mixinMultiLangTextWithValidLanguages");
    @NullAllowed
    private final MultiLanguageString mixinMultiLangTextWithValidLanguages =
            new MultiLanguageString(new ArrayList<>(MongoMultiLanguageStringEntity.validLanguages));

    public MultiLanguageString getMixinMultiLangTextWithValidLanguages() {
        return mixinMultiLangTextWithValidLanguages;
    }
}
