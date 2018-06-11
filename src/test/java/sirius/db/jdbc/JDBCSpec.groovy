/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Limit
import sirius.kernel.di.std.Part
import spock.lang.Stepwise

import java.util.function.Function

@Stepwise
class JDBCSpec extends BaseSpecification {

    @Part
    static Databases dbs

    def "test database is loaded from config while profile is applied"() {
        when:
        def db = dbs.get("test")
        then:
        db.createQuery("SELECT 1").queryList().size() == 1
    }

    def "create table for 'test_a' works"() {
        given:
        def db = dbs.get("test")
        when: "a create statement is submitted"
        db.createQuery("CREATE TABLE test_a(a CHAR(10), b INT DEFAULT 1)").executeUpdate()
        then: "an empty table was create"
        db.createQuery("SELECT * FROM test_a").queryList().size() == 0
    }

    def "insert works on test table 'test_a'"() {
        given:
        def db = dbs.get("test")
        when: "a insert statements are submitted"
        db.insertRow("test_a", [a : 'Hello'])
        db.insertRow("test_a", [a : 'Test', b: 2])
        then: "data is returned from the database"
        db.createQuery("SELECT * FROM test_a").queryList().size() == 2
        and:
        db.createQuery("SELECT * FROM test_a ORDER BY a ASC").queryFirst().getValue("A").asString() == "Hello"
    }

    def "an IllegalArgumentException is created if an unknown column is selected"() {
        when:
        def r = new Row()
        and:
        r.getValue("X")
        then:
        thrown(IllegalArgumentException)
    }

    def "queryList returns all inserted rows"() {
        given:
        def db = dbs.get("test")
        when:
        def qry = db.createQuery('SELECT * FROM test_a')
        then:
        qry.queryList().size() == 2
    }

    def "SQLQuery#queryFirst returns a row"() {
        given:
        def db = dbs.get("test")
        when:
        def qry = db.createQuery('SELECT * FROM test_a ORDER BY a ASC')
        then:
        qry.queryFirst().getValue("a").asString() == "Hello"
    }

    def "SQLQuery#queryFirst returns null for an empty result set"() {
        given:
        def db = dbs.get("test")
        when:
        def qry = db.createQuery("SELECT a,b FROM test_a WHERE a = 'xxx'")
        then:
        qry.queryFirst() == null
    }

    def "SQLQuery#first returns an empty optional"() {
        given:
        def db = dbs.get("test")
        when:
        def qry = db.createQuery("SELECT * FROM test_a WHERE a = 'xxx'")
        then:
        !qry.first().isPresent()
    }

    def "SQLQuery#executeUpdate works changes a row"() {
        given:
        def db = dbs.get("test")
        when:
        int numberOfRowsChanged = db.createQuery("UPDATE test_a SET a = 'xxx' WHERE a = 'Test'").executeUpdate()
        then:
        numberOfRowsChanged == 1
        and:
        db.createQuery("SELECT * FROM test_a WHERE a = 'xxx'").first().isPresent()
    }

    def "the statement compiler omits an empty clause"() {
        given:
        def db = dbs.get("test")
        when:
        def qry = db.createQuery('SELECT * FROM test_a [WHERE a = ${filter}]').set("filter", null)
        then:
        qry.queryList().size() == 2
    }

    def "the statement compiler includes an non-empty clause"() {
        given:
        def db = dbs.get("test")
        when:
        def qry = db.createQuery('SELECT * FROM test_a [WHERE a = ${filter}]').set("filter", "Hello")
        then:
        qry.queryList().size() == 1
    }

    def "the statement compiler expands hash-fields correctly"() {
        given:
        def db = dbs.get("test")
        when:
        def qry = db.createQuery('SELECT * FROM test_a WHERE LOWER(a) LIKE #{filter}').set("filter", "HEL")
        then:
        qry.queryList().size() == 1
    }

    def "SQLQuery#iterate is evaluated correctly"() {
        given:
        def db = dbs.get("test")
        when:
        def qry = db.createQuery('SELECT a,b FROM test_a')
        then:
        qry.iterate({ it.getFieldsList().size() == 2 } as Function, Limit.UNLIMITED)
    }

}
