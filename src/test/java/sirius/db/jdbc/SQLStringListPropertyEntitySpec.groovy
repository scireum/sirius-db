/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class SQLStringListPropertyEntitySpec extends BaseSpecification {

    @Part
    static OMA oma

    def "test many list entries"() {
        when:
        SQLStringListPropertyEntity entity = new SQLStringListPropertyEntity()
        entity.getStringList().add("test1").add("test2").add("test3").add("test4")
        and:
        oma.update(entity)
        then:
        def result = oma.find(SQLStringListPropertyEntity.class, entity.getId()).get()
        result.getStringList().size() == 4
        result.getStringList().contains("test1") == true
        result.getStringList().contains("test2") == true
        result.getStringList().contains("test3") == true
        result.getStringList().contains("test4") == true
    }

    def "test one list entry"() {
        when:
        SQLStringListPropertyEntity entity = new SQLStringListPropertyEntity()
        entity.getStringList().add("test1")
        and:
        oma.update(entity)
        then:
        def result = oma.find(SQLStringListPropertyEntity.class, entity.getId()).get()
        result.getStringList().size() == 1
        result.getStringList().contains("test1") == true
    }

    def "test no list enties"() {
        when:
        SQLStringListPropertyEntity entity = new SQLStringListPropertyEntity()
        and:
        oma.update(entity)
        then:
        def result = oma.find(SQLStringListPropertyEntity.class, entity.getId()).get()
        result.getStringList().size() == 0
    }
}
