/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import sirius.db.mixing.Mapping
import sirius.db.mixing.Mixing
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

import java.time.Duration

class LegacySpec extends BaseSpecification {

    @Part
    private static OMA oma

            def setupSpec() {
        oma.getReadyFuture().await(Duration.ofSeconds(60))
    }

    def "check if aliasing for columns work"() {
        given:
        LegacyEntity e = new LegacyEntity()
        when:
        e.setFirstname("Test")
        e.setLastname("Entity")
        e.getComposite().setStreet("Street")
        e.getComposite().setCity("Test-City")
        e.getComposite().setZip("1245")
        oma.update(e)
        LegacyEntity fromDB = oma.select(LegacyEntity.class)
                .eq(Mapping.named("firstname"), "Test")
                .orderAsc(Mapping.named("composite").inner(Mapping.named("street")))
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
        oma.getDatabase(Mixing.DEFAULT_REALM).
        createQuery("SELECT * FROM banana WHERE name1 = 'Test' and street = 'Street'").
        first().
        isPresent()
    }

    def "updating and change tracking on an entity with aliased columns works"() {
        given:
        LegacyEntity e = new LegacyEntity()
        when:
        e.setFirstname("Test2")
        e.setLastname("Entity2")
        e.getComposite().setStreet("Streeet2")
        e.getComposite().setCity("Test-City2")
        e.getComposite().setZip("12452")
        oma.update(e)
        and:
        e.getComposite().setStreet("Street3")
        oma.update(e)
        and:
        e = oma.refreshOrFail(e)
        then:
        e.getComposite().getStreet() == "Street3"
    }

}
