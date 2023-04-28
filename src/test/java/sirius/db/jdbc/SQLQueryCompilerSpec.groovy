/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc

import sirius.db.jdbc.constraints.SQLConstraint
import sirius.db.jdbc.constraints.SQLQueryCompiler
import sirius.db.mixing.Mapping
import sirius.db.mixing.Mixing
import sirius.db.mixing.Property
import sirius.db.mixing.query.QueryCompiler
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
        queryCompiler.compile().toString() == "firstname IS NULL"
    }

    def "compiling '!firstname:' yields a constraint"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "!firstname:",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "NOT(firstname IS NULL)"
    }

    def "compiling '-firstname:' yields a constraint"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "-firstname:",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "NOT(firstname IS NULL)"
    }

    def "compiling 'firstname:X OR lastname:Y' works as expected"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "firstname:X OR lastname:Y",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "(firstname = X OR lastname = Y)"
    }

    def "compiling 'firstname:X AND lastname:Y' works as expected"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "firstname:X AND lastname:Y",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "(firstname = X AND lastname = Y)"
    }

    def "compiling '!firstname:X OR lastname:Y' works as expected"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "!firstname:X OR lastname:Y",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "(NOT(firstname = X) OR lastname = Y)"
    }

    def "compiling '-firstname:X OR lastname:Y' works as expected"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "-firstname:X OR lastname:Y",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "(NOT(firstname = X) OR lastname = Y)"
    }

    def "compiling '!(firstname:X OR lastname:Y)' works as expected"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "!(firstname:X OR lastname:Y)",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "NOT((firstname = X OR lastname = Y))"
    }

    def "compiling '-(firstname:X OR lastname:Y)' works as expected"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "-(firstname:X OR lastname:Y)",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "NOT((firstname = X OR lastname = Y))"
    }

    def "compiling '-X' works as expected"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "-X",
                Arrays.asList(QueryField.eq(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "NOT(firstname = X)"
    }

    def "compiling '\"-X\"' works as expected"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "\"-X\"",
                Arrays.asList(QueryField.eq(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "firstname = -X"
    }

    def "compiling 'Y-X' works as expected"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "Y-X",
                Arrays.asList(QueryField.eq(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "firstname = Y-X"
    }

    def "compiling 'firstname:test' works as expected"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "firstname:test",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "firstname = test"
    }

    def "compiling 'firstname:type:value-123' works"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "firstname:type:value-123",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "firstname = type:value-123"

    }

    def "compiling 'firstname:type(value-123)' works"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "firstname:type(value-123)",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "firstname = type(value-123)"

    }

    def "compiling 'hello:world' does not treat hello as a field"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "hello:world",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "LOWER(firstname) LIKE '%hello:world%'"

    }

    def "compiling 'hello::world' does not treat hello as a field"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "hello::world",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "LOWER(firstname) LIKE '%hello::world%'"

    }

    def "compiling 'hello > world' silently drops the operator as hello isn't a field"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "hello > world",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME)))
        then:
        queryCompiler.compile().toString() == "(LOWER(firstname) LIKE '%hello%' AND LOWER(firstname) LIKE '%world%')"
    }

    def "compiling 'parent.name:Test' compiles into a JOIN FETCH"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(SmartQueryTestChildEntity.class),
                "parent.name:Test",
                Collections.emptyList())
        then:
        queryCompiler.compile().toString() == "parent.name = Test"
    }

    def "compiling 'parent.unknownProperty:Test' reports an appropriate error"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(SmartQueryTestChildEntity.class),
                "parent.unknownProperty:Test",
                Collections.emptyList())
        and:
        queryCompiler.compile()
        then: "If an unknown property is accessed and no search fields exist, an error is reported"
        thrown(IllegalArgumentException)
    }

    def "customizing constraint compilation works"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "is:chat",
                Arrays.asList(QueryField.contains(TestEntity.FIRSTNAME))) {
            @Override
            protected SQLConstraint compileCustomField(String field) {
                return parseOperation(Mapping.named(field), null)
            }

            @Override
            protected QueryCompiler.FieldValue compileValue(Property property, QueryCompiler.FieldValue value) {
                return value
            }

        }
        then:
        queryCompiler.compile().toString() == "is = chat"
    }

    def "compiling a field with OR in its name works"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "firstname: x orderNumber: 1",
                Collections.emptyList())
        then:
        queryCompiler.compile().toString() == "(firstname = x AND orderNumber = 1)"
    }

    def "compiling a field with AND in its name works"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "firstname: x andx: 1",
                Collections.emptyList())
        then:
        queryCompiler.compile().toString() == "(firstname = x AND andx = 1)"
    }

    def "compiling 'foo - bar' works as expected"() {
        when:
        SQLQueryCompiler queryCompiler = new SQLQueryCompiler(
                OMA.FILTERS,
                mixing.getDescriptor(TestEntity.class),
                "foo - bar",
                Arrays.asList((QueryField.eq(TestEntity.FIRSTNAME))))
        then:
        queryCompiler.compile().toString() == "(firstname = foo AND firstname = bar)"
    }

}
