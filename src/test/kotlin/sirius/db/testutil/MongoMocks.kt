/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.testutil

import org.mockito.Mockito.spy
import sirius.db.mixing.types.BaseEntityRef
import sirius.db.mongo.MongoEntity
import sirius.db.mongo.types.MongoRef

/**
 * Class contains functionality used for mocking Mongo parts in unit tests.
 */
class MongoMocks {
    companion object {
        /**
         * Wrap an entity as a MongoRef
         * @param entity which is to be wrapped
         */
        fun <E : MongoEntity> asMongoRef(entity: E): MongoRef<E> {
            val mongoRef = spy(MongoRef.on(entity.javaClass, BaseEntityRef.OnDelete.IGNORE))
            mongoRef.setValue(entity)
            return mongoRef
        }
    }
}
