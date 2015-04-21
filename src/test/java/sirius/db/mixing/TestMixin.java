/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.mixing.Mixable;
import sirius.mixing.annotations.Length;
import sirius.mixing.annotations.Mixin;

@Mixin(TestEntityWithMixin.class)
public class TestMixin extends Mixable {
    @Length(length = 50)
    private String middleName;

    public String getMiddleName() {
        return middleName;
    }

    public void setMiddleName(String middleName) {
        this.middleName = middleName;
    }
}
