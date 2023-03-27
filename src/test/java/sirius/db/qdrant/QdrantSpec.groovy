/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.qdrant

import sirius.db.util.Tensors
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class QdrantSpec extends BaseSpecification {

    @Part
    private static Qdrant qdrant

    def "create collection works"() {
        when:
        qdrant.db().createCollection("test", 10, QdrantDatabase.Similarity.COSINE)
        then:
        qdrant.db().collectionExists("test")
    }

    def "insert works"() {
        given:
        qdrant.db().deleteCollection("test")
        and:
        qdrant.db().ensureCollectionExists("test", 10, QdrantDatabase.Similarity.COSINE)
        when:
        qdrant.db().upsert("test", [
                new Point(Point.deriveId("1"), Tensors.fromList([1, 0, 0, 0, 0, 0, 0, 0, 0, 0])),
                new Point(Point.deriveId("2"), Tensors.fromList([2, 0, 0, 0, 0, 0, 0, 0, 0, 0])),
                new Point(Point.deriveId("3"), Tensors.fromList([3, 0, 0, 0, 0, 0, 0, 0, 0, 0]))
        ])
        then:
        qdrant.db().countPoints("test", true) == 3
    }

    def "query works"() {
        given:
        qdrant.db().deleteCollection("test-search")
        and:
        qdrant.db().ensureCollectionExists("test-search", 10, QdrantDatabase.Similarity.COSINE)
        when:
        qdrant.db().upsert("test-search", [
                new Point(Point.deriveId("1"), Tensors.fromList([1, 0, 0, 0, 0, 0, 0, 0, 0, 0])).withPayload("x", 1),
                new Point(Point.deriveId("2"), Tensors.fromList([2, 0, 2, 0, 0, 0, 0, 0, 0, 0])).withPayload("x", 2),
                new Point(Point.deriveId("3"), Tensors.fromList([3, 0, 3, 0, 7, 0, 0, 0, 0, 0])).withPayload("x", 3)
        ])
        and:
        def result = qdrant.db().query("test-search", Tensors.fromList([1, 0, 0, 0, 0, 0, 0, 0, 0, 0]))
                           .queryPayload(Integer.class, "x", 2)
        then:
        result.size() == 2
        and:
        result.get(0) == 1
        and:
        result.get(1) == 2
    }

}
