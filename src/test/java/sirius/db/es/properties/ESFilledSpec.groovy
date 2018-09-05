/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es.properties

import sirius.db.es.Elastic
import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Wait
import sirius.kernel.di.std.Part

class ESFilledSpec extends BaseSpecification {

    @Part
    private static Elastic elastic

    def "filled/notFilled/exists query works"() {
        setup:
        ESFilledEntity fieldFilled = new ESFilledEntity()
        fieldFilled.setTestField("test")
        ESFilledEntity fieldNotFilled = new ESFilledEntity()
        when:
        elastic.update(fieldFilled)
        elastic.update(fieldNotFilled)
        Wait.seconds(1)
        then:
        elastic.select(ESFilledEntity.class)
               .eq(ESFilledEntity.TEST_FIELD, null)
               .queryFirst()
               .getIdAsString() == fieldNotFilled.getIdAsString() && elastic.select(ESFilledEntity.class)
                                                                            .eq(ESFilledEntity.TEST_FIELD, null).count() == 1
        elastic.select(ESFilledEntity.class)
               .where(Elastic.FILTERS.notFilled(ESFilledEntity.TEST_FIELD))
               .queryFirst()
               .getIdAsString() == fieldNotFilled.getIdAsString() && elastic.select(ESFilledEntity.class)
                                                                            .where(Elastic.FILTERS.notFilled(ESFilledEntity.TEST_FIELD)).count() == 1
        elastic.select(ESFilledEntity.class)
               .where(Elastic.FILTERS.notExists(ESFilledEntity.TEST_FIELD))
               .queryFirst()
               .getIdAsString() == fieldNotFilled.getIdAsString() && elastic.select(ESFilledEntity.class)
                                                                            .where(Elastic.FILTERS.notExists(ESFilledEntity.TEST_FIELD)).count() == 1

        elastic.select(ESFilledEntity.class)
               .ne(ESFilledEntity.TEST_FIELD, null)
               .queryFirst()
               .getIdAsString() == fieldFilled.getIdAsString() && elastic.select(ESFilledEntity.class)
                                                                         .ne(ESFilledEntity.TEST_FIELD, null).count() == 1
        elastic.select(ESFilledEntity.class)
               .where(Elastic.FILTERS.filled(ESFilledEntity.TEST_FIELD))
               .queryFirst()
               .getIdAsString() == fieldFilled.getIdAsString() && elastic.select(ESFilledEntity.class)
                                                                         .where(Elastic.FILTERS.filled(ESFilledEntity.TEST_FIELD)).count() == 1
        elastic.select(ESFilledEntity.class)
               .where(Elastic.FILTERS.exists(ESFilledEntity.TEST_FIELD))
               .queryFirst()
               .getIdAsString() == fieldFilled.getIdAsString() && elastic.select(ESFilledEntity.class)
                                                                         .where(Elastic.FILTERS.exists(ESFilledEntity.TEST_FIELD)).count() == 1
    }
}
