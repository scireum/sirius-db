/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing

import sirius.db.es.Elastic
import sirius.db.jdbc.OMA
import sirius.db.mongo.Mango
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException

import java.time.Duration

class CompositeSpec extends BaseSpecification {

    @Part
    private static Elastic elastic

    @Part
    private static OMA oma

    @Part
    private static Mango mango

    def "Updating Composite inside Mixable works on Mongo"() {
        when:
        RefMongoEntity refMongoEntity = new RefMongoEntity()
        refMongoEntity.as(MongoMixable.class).getComposite().getMap().modify().put("test", "data")
        mango.update(refMongoEntity)
        then:
        notThrown(HandledException)
    }

}
