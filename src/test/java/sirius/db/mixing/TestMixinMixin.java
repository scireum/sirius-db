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

@Mixin(TestMixin.class)
public class TestMixinMixin extends Mixable {
    @Length(length = 1)
    private String initial;

    public String getInitial() {
        return initial;
    }

    public void setInitial(String initial) {
        this.initial = initial;
    }
}
