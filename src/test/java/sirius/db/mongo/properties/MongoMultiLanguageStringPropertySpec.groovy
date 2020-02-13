/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.properties

import sirius.db.mongo.Mango
import sirius.db.mongo.Mongo
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException

class MongoMultiLanguageStringPropertySpec extends BaseSpecification {

    @Part
    private static Mango mango

    @Part
    private static Mongo mongo

    def "invalid language"() {
        given:
        def entity = new MongoMultiLanguageStringEntity()
        when:
        entity.getMultiLangText().addText("00", "")
        then:
        thrown(HandledException)
    }

    def "store retrieve and validate"() {
        given:
        def entity = new MongoMultiLanguageStringEntity()
        entity.getMultiLangText().addText("de", "Schmetterling")
        entity.getMultiLangText().addText("en", "Butterfly")
        mango.update(entity)

        when:
        def output = mango.refreshOrFail(entity)

        then:
        output.getMultiLangText().size() == 2
        output.getMultiLangText().hasText("de")
        !output.getMultiLangText().hasText("fr")
        output.getMultiLangText().fetchText("de") == "Schmetterling"
        output.getMultiLangText().fetchText("fr") == null
        output.getMultiLangText().fetchText("de", "en") == "Schmetterling"
        output.getMultiLangText().fetchText("fr", "en") == "Butterfly"
        output.getMultiLangText().fetchText("fr", "es") == null
        output.getMultiLangText().getText("de") == Optional.of("Schmetterling")
        output.getMultiLangText().getText("fr") == Optional.empty()
        output.getMultiLangText().getRequiredText("de") == "Schmetterling"

        when:
        output.getMultiLangText().getRequiredText("fr")
        then:
        thrown(HandledException)

        when:
        output.getMultiLangText().setDefaultLanguage("en")

        then:
        output.getMultiLangText().getRequiredText() == "Butterfly"
        output.getMultiLangText().fetchText() == "Butterfly"
        output.getMultiLangText().fetchText("es", "fr") == "Butterfly"
        output.getMultiLangText().getText() == Optional.of("Butterfly")
    }

    def "raw data check"() {
        given:
        def entity = new MongoMultiLanguageStringEntity()
        entity.getMultiLangText().addText("pt", "Borboleta")
        entity.getMultiLangText().addText("es", "Mariposa")
        mango.update(entity)

        when:
        def expectedString = "[Document{{lang=pt, text=Borboleta}}, Document{{lang=es, text=Mariposa}}]"
        def storedString = mongo.find()
                                .where("id", entity.getId())
                                .singleIn("mongomultilanguagestringentity")
                                .get()
                                .get("multiLangText")
                                .asString()

        then:
        expectedString == storedString
    }
}
