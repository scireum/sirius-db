/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.fieldlookup;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.annotations.Length;
import sirius.db.mixing.annotations.Mixin;
import sirius.db.mixing.types.StringList;

@Mixin(SQLFieldLookUpTestEntity.class)
public class SQLSuperHeroTestMixin extends Mixable {

    public static final Mapping HERO_NAMES = Mapping.named("heroNames");
    private final NameFieldsTestComposite heroNames = new NameFieldsTestComposite();

    public static final Mapping SUPER_POWERS = Mapping.named("superPowers");
    @Length(100)
    private final StringList superPowers = new StringList();

    public NameFieldsTestComposite getHeroNames() {
        return heroNames;
    }

    public StringList getSuperPowers() {
        return superPowers;
    }
}
