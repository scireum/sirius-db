/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.db.mixing.annotations.Length;
import sirius.db.mixing.annotations.Mixin;

@Mixin(SmartQueryTestMixinEntity.class)
public class SmartQueryTestMixinEntityMixin extends Mixable {

    @Length(50)
    private String mixinValue;
    public static final Column MIXIN_VALUE = Column.named("mixinValue");

    public String getMixinValue() {
        return mixinValue;
    }

    public void setMixinValue(String mixinValue) {
        this.mixinValue = mixinValue;
    }
}
