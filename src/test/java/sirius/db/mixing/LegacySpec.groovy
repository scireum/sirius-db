/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing

import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class LegacySpec extends BaseSpecification {

    @Part
    private static OMA oma;

    def "check if aliasing for columns work"() {
        given:
        LegacyEntity e = new LegacyEntity();
        when:
        e.setFirstname("Test");
        e.setLastname("Entity");
        e.getComposite().setStreet("Street");
        e.getComposite().setCity("Test-City");
        e.getComposite().setZip("1245");
        oma.update(e);
        LegacyEntity fromDB = oma.select(LegacyEntity.class)
                .eq(Column.named("firstname"), "Test")
                .orderAsc(Column.named("composite").inner(Column.named("street")))
                .queryFirst()
        then:
        !e.isNew()
        and:
        fromDB != null
        and:
        fromDB.getFirstname() == "Test"
        and:
        fromDB.getComposite().getStreet() == "Street"
        and:
        oma.getDatabase().createQuery("SELECT * FROM banana WHERE name1 = 'Test' and street = 'Street'").first().isPresent()
    }

}
