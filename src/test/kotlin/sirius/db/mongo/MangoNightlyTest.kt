/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo

import org.junit.jupiter.api.Tag
import sirius.kernel.BaseSpecification
import sirius.kernel.Tags
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException

@Tag(Tags.NIGHTLY)
class MangoNightlySpec extends BaseSpecification {

    @Part
    private static Mango mango

            def "selecting over 1000 entities in queryList throws an exception"() {
        given:
        mango.select(MangoListTestEntity.class).delete()
                and:
                for (int i = 0; i < 1001; i++) {
        def entityToCreate = new MangoListTestEntity()
        entityToCreate.setCounter(i)
        mango.update(entityToCreate)
    }
        when:
        mango.select(MangoListTestEntity.class).queryList()
                then:
                thrown(HandledException)
    }

    def "a timed out mongo count returns an empty optional"() {
        when:
        mango.select(MangoListTestEntity.class).delete()
                and:
                for (int i = 0; i < 100_000; i++) {
        def entityToCreate = new MangoListTestEntity()
        entityToCreate.setCounter(i)
        mango.update(entityToCreate)
    }
        and:
        MongoQuery<MangoListTestEntity> query = mango
                .select(MangoListTestEntity.class)
                then:
                query.count(true, 1) == Optional.empty()
    }


}
