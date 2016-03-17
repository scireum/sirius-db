/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing;

import sirius.mixing.Entity;
import sirius.mixing.annotations.Length;

public class LegacyEntity extends Entity {

    @Length(length = 50)
    private String firstname;

    @Length(length = 50)
    private String lastname;

    private final TestComposite composite = new TestComposite();

    public TestComposite getComposite() {
        return composite;
    }

    public String getFirstname() {
        return firstname;
    }

    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    public String getLastname() {
        return lastname;
    }

    public void setLastname(String lastname) {
        this.lastname = lastname;
    }
}
