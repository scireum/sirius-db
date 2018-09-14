/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import sirius.db.jdbc.constraints.SQLQueryCompiler
import sirius.db.mixing.Mixing
import sirius.db.mixing.query.QueryField
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

class SQLQueryCompilerSpec extends BaseSpecification {

    @Part
    private static Mixing mixing

    def "compiling '' works"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile() == null
    }

    def "compiling ':' yields an empty constraint"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                ":",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile() == null
    }

    def "compiling '=' yields an empty constraint"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "=",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile() == null
    }

    def "compiling 'firstname:' works as expected"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "firstname:",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "((firstname IS NULL))"
    }

    def "compiling '!firstname:' yields a constraint"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "!firstname:",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "((NOT(firstname IS NULL)))"
    }

    def "compiling '!firstname:X OR lastname:Y' works as expected"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "!firstname:X OR lastname:Y",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "((NOT(firstname = X)) OR (lastname = Y))"
    }

    def "compiling '!(firstname:X OR lastname:Y)' works as expected"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "!(firstname:X OR lastname:Y)",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "((NOT((firstname = X) OR (lastname = Y)))"
    }

    def "compiling 'firstname:test' works as expected"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "firstname:test",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "((firstname = test))"
    }
}
