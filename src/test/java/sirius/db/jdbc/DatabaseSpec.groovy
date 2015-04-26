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
import spock.lang.Stepwise

import java.util.function.Function

@Stepwise
class DatabaseSpec extends BaseSpecification {

    @Part
    static Databases dbs;

    def "test database is loaded from config while profile is applied"() {
        when:
        def db = dbs.get("test");
        then:
        db.createQuery("SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS").queryList().size() == 1
        and: "no connection is active"
        db.getNumActive() == 0
        and: "one connection resides in the pool"
        db.getNumIdle() == 1
    }

    def "create table for 'test_a' works on in-memory HSQLDB"() {
        given:
        def db = dbs.get("test");
        when: "a create statement is submitted"
        db.createQuery("CREATE TABLE test_a(a CHAR(10), b INT DEFAULT 1)").executeUpdate();
        then: "an empty table was create"
        db.createQuery("SELECT * FROM test_a").queryList().size() == 0
    }

    def "insert works on test table 'test_a'"() {
        given:
        def db = dbs.get("test");
        when: "a insert statements are submitted"
        db.insertRow("test_a", [a : 'Hello']);
        db.insertRow("test_a", [a : 'Test', b: 2]);
        then: "data is returned from the database"
        db.createQuery("SELECT * FROM test_a").queryList().size() == 2
        and:
        db.createQuery("SELECT * FROM test_a ORDER BY a ASC").queryFirst().getValue("A").asString() == "Hello"
    }

    def "an IllegalArgumentException is created if an unknown column is selected"() {
        when:
        def r = new Row();
        and:
        r.getValue("X")
        then:
        thrown(IllegalArgumentException)
    }

    def "the statement compiler omits an empty clause"() {
        given:
        def db = dbs.get("test");
        when:
        def qry = db.createQuery('SELECT * FROM test_a [WHERE a = ${filter}]').set("filter", null);
        then:
        qry.queryList().size() == 2
    }

    def "the statement compiler includes an non-empty clause"() {
        given:
        def db = dbs.get("test");
        when:
        def qry = db.createQuery('SELECT * FROM test_a [WHERE a = ${filter}]').set("filter", "Hello");
        then:
        qry.queryList().size() == 1
    }

    def "the statement compiler expands hash-fields correctly"() {
        given:
        def db = dbs.get("test");
        when:
        def qry = db.createQuery('SELECT * FROM test_a WHERE LOWER(a) LIKE #{filter}').set("filter", "HEL");
        then:
        qry.queryList().size() == 1
    }

    def "SQLQuery#perform is evaluated correctly"() {
        given:
        def db = dbs.get("test");
        when:
        def qry = db.createQuery('SELECT a,b FROM test_a');
        then:
        qry.iterate({ it.getFields().size() == 2 } as Function, 0)
    }

    def "SQLQuery#queryFirst returns null for an empty result set"() {
        given:
        def db = dbs.get("test");
        when:
        def qry = db.createQuery("SELECT a,b FROM test_a WHERE a = 'XXX'");
        then:
        qry.queryFirst() == null
    }

}
