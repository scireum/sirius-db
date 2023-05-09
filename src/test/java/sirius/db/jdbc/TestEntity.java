/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.Index;
import sirius.db.mixing.annotations.Length;

@Index(name = "lastname", columns = "lastname")
public class TestEntity extends SQLEntity {

    public static final Mapping FIRSTNAME = Mapping.named("firstname");
    @Length(50)
    private String firstname;

    public static final Mapping LASTNAME = Mapping.named("lastname");
    @Length(50)
    private String lastname;

    public static final Mapping AGE = Mapping.named("age");
    private int age;

    public static final Mapping ORDER_NUMBER = Mapping.named("orderNumber");
    private int orderNumber = 0;

    public static final Mapping ANDX = Mapping.named("andx");
    private int andx = 0;

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

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
