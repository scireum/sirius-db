/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import sirius.db.mixing.Mixing
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

import java.time.Duration
import java.util.function.Function
import java.util.stream.Collectors

class TransformedQuerySpec extends BaseSpecification {

    @Part
    static OMA oma

    def setupSpec() {
        oma.getReadyFuture().await(Duration.ofSeconds(60))

        TransformedQueryTestEntity e = new TransformedQueryTestEntity()
        e.setValue("Test")
        oma.update(e)
        e = new TransformedQueryTestEntity()
        e.setValue("Hello")
        oma.update(e)
        e = new TransformedQueryTestEntity()
        e.setValue("World")
        oma.update(e)
    }

    def "transform works when reading a test entity"() {
        given:
        SQLQuery qry = oma.getDatabase(Mixing.DEFAULT_REALM).
        createQuery("SELECT * FROM transformedquerytestentity ORDER BY value ASC")
        when:
        def e = oma.transform(TransformedQueryTestEntity.class, qry).queryFirst()
        and:
        def es = oma.transform(TransformedQueryTestEntity.class, qry).queryList()
        then:
        e.getValue() == "Hello"
        and:
        es.stream().map({ x -> x.getValue() } as Function).collect(Collectors.toList()) == ["Hello", "Test", "World"]
    }

    def "transform works when reading a test entity with alias"() {
        given:
        SQLQuery qry = oma.getDatabase(Mixing.DEFAULT_REALM).
        createQuery("SELECT id as x_id, value as x_value  FROM transformedquerytestentity ORDER BY value ASC")
        when:
        def e = oma.transform(TransformedQueryTestEntity.class, "x", qry).first()
        then:
        e.isPresent()
        and:
        e.get().getValue() == "Hello"
    }

    def "transform works when reading a test entity with a computed column"() {
        given:
        SQLQuery qry = oma.getDatabase(Mixing.DEFAULT_REALM).
        createQuery("SELECT id, value, 'x' as test FROM transformedquerytestentity ORDER BY value ASC")
        when:
        def e = oma.transform(TransformedQueryTestEntity.class, qry).queryFirst()
        then:
        e.getValue() == "Hello"
        and:
        e.getFetchRow().getValue("test").asString() == "x"
    }

}
