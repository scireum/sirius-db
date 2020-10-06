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
import sirius.kernel.health.HandledException

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

    def "test exception is thrown, if the list is to long for the field"() {
        when:
        SQLStringListPropertyEntity entity = new SQLStringListPropertyEntity()
        entity.getShortStringList().add("test1").add("test2").add("test3").add("test4")
        and:
        oma.update(entity)
        then:
        def e = thrown(HandledException.class)
        e.getMessage() == "Der Wert 'test1,test2,test3,test4' im Feld 'Model.shortStringList' ist mit 23 Zeichen zu " +
                "lang. Maximal sind 20 Zeichen erlaubt."
    }
}
