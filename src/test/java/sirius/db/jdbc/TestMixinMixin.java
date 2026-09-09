/*
 * Made with all the love in the world
 * by scireum in Stuttgart, Germany
 *
 * Copyright by scireum GmbH
 * https://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.Mixable;
import sirius.db.mixing.annotations.Length;
import sirius.db.mixing.annotations.Mixin;

@Mixin(TestMixin.class)
public class TestMixinMixin extends Mixable {
    @Length(1)
    private String initial;

    public String getInitial() {
        return initial;
    }

    public void setInitial(String initial) {
        this.initial = initial;
    }
}
