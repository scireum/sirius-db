/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.fieldlookup;

import sirius.db.jdbc.SQLEntity;
import sirius.db.mixing.Mapping;
import sirius.db.mixing.annotations.Length;
import sirius.db.mixing.annotations.NullAllowed;
import sirius.db.mixing.types.StringList;

import java.time.LocalDateTime;

public class SQLFieldLookUpTestEntity extends SQLEntity {

    public static final Mapping NAMES = Mapping.named("names");
    private final NameFieldsTestComposite names = new NameFieldsTestComposite();

    public static final Mapping AGE = Mapping.named("age");
    private int age;

    public static final Mapping COOL = Mapping.named("cool");
    private boolean cool;

    public static final Mapping BIRTHDAY = Mapping.named("birthday");
    @NullAllowed
    private LocalDateTime birthday;

    public static final Mapping SUPER_POWERS = Mapping.named("superPowers");
    @Length(100)
    @NullAllowed
    private final StringList superPowers = new StringList();

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

    public NameFieldsTestComposite getNames() {
        return names;
    }
}
