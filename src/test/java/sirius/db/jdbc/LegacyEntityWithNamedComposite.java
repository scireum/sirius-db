/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.Length;

public class LegacyEntityWithNamedComposite extends SQLEntity {

    public static final Mapping FIRSTNAME = Mapping.named("firstname");
    @Length(50)
    private String firstname;

    public static final Mapping LASTNAME = Mapping.named("lastname");
    @Length(50)
    private String lastname;

    public static final Mapping ADDRESSDATA = Mapping.named("addressdata");
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
