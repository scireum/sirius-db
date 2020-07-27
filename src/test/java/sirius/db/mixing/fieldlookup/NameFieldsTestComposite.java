/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.fieldlookup;

import sirius.db.mixing.Composite;
import sirius.db.mixing.Mapping;

public class NameFieldsTestComposite extends Composite {

    public static final Mapping FIRSTNAME = Mapping.named("firstname");
    private String firstname;

    public static final Mapping LASTNAME = Mapping.named("lastname");
    private String lastname;

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
