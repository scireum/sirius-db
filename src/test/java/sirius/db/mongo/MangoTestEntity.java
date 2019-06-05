/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.NullAllowed;

import java.time.LocalDateTime;

public class MangoTestEntity extends MongoEntity {

    public static final Mapping FIRSTNAME = Mapping.named("firstname");
    private String firstname;

    public static final Mapping LASTNAME = Mapping.named("lastname");
    private String lastname;

    public static final Mapping AGE = Mapping.named("age");
    private int age;

    public static final Mapping COOL = Mapping.named("cool");
    private boolean cool;

    public static final Mapping BIRTHDAY = Mapping.named("birthday");
    @NullAllowed
    private LocalDateTime birthday;

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

    public boolean isCool() {
        return cool;
    }

    public void setCool(boolean cool) {
        this.cool = cool;
    }

    public LocalDateTime getBirthday() {
        return birthday;
    }

    public void setBirthday(LocalDateTime birthday) {
        this.birthday = birthday;
    }
}
