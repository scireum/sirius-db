/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.Mixable;
import sirius.db.mixing.annotations.Length;
import sirius.db.mixing.annotations.Mixin;

@Mixin(SmartQueryTestMixinEntity.class)
public class SmartQueryTestMixinEntityMixin extends Mixable {

    @Length(50)
    private String mixinValue;
    public static final Mapping MIXIN_VALUE = Mapping.named("mixinValue");

    public String getMixinValue() {
        return mixinValue;
    }

    public void setMixinValue(String mixinValue) {
        this.mixinValue = mixinValue;
    }
}
