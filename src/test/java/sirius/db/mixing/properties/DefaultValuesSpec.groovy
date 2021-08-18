/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties

import sirius.db.mixing.Mixing
import sirius.db.mixing.MongoDefaultValuesEntity
import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Amount
import sirius.kernel.commons.Value
import sirius.kernel.di.std.Part

class DefaultValuesSpec extends BaseSpecification {

    @Part
    private static Mixing mixing

    def "the default values are properly initialized"() {
        expect:
        mixing.getDescriptor(MongoDefaultValuesEntity.class).findProperty(propertyName).getDefaultValue() ==
                excpectedDefault

        where:
        propertyName            | excpectedDefault
        "primitiveBoolean"      | "false"
        "primitiveBooleanTrue"  | "true"
        "booleanObject"         | null
        "primitiveInt"          | "0"
        "primitiveIntWithValue" | "50"
        "amount"                | null
        "amountWithValue"       | "100.00"
        "amountZero"            | "0.00"
        "amountNothing"         | null
        "string"                | null
        "emptyString"           | ""
        "enumWithValue"         | "Test2"
    }

    def "a primitive boolean with no default value annotation does not throw an error"() {
        given:
        def property = mixing.getDescriptor(MongoDefaultValuesEntity.class).findProperty("primitiveBoolean")
        and:
        MongoDefaultValuesEntity entity = new MongoDefaultValuesEntity()
        entity.setPrimitiveBoolean(true)
        when: // an empty value is given, its default (false) should be applied
        property.parseValueFromImport(entity, Value.EMPTY)
        then:
        !entity.isPrimitiveBoolean()
    }

    def "a primitive boolean gets its default value from its initial assigned value"() {
        given:
        def property = mixing.getDescriptor(MongoDefaultValuesEntity.class).findProperty("primitiveBooleanTrue")
        and:
        MongoDefaultValuesEntity entity = new MongoDefaultValuesEntity()
        when: // an empty value is given, its default (true) should be applied
        property.parseValueFromImport(entity, Value.EMPTY)
        then:
        entity.isPrimitiveBooleanTrue()
    }

    def "a boolean object field can have no default value"() {
        given:
        def property = mixing.getDescriptor(MongoDefaultValuesEntity.class).findProperty("booleanObject")
        and:
        MongoDefaultValuesEntity entity = new MongoDefaultValuesEntity()
        entity.setBooleanObject(Boolean.TRUE)
        when: // an empty value is given, the field should reset to 'null'
        property.parseValueFromImport(entity, Value.EMPTY)
        then:
        entity.getBooleanObject() == null
    }

    def "primitive number fields get automatic default value from their initial default value"() {
        given:
        def property = mixing.getDescriptor(MongoDefaultValuesEntity.class).findProperty("primitiveInt")
        and:
        MongoDefaultValuesEntity entity = new MongoDefaultValuesEntity()
        entity.setPrimitiveInt(12)
        when: // an empty value is given, its default (0) should be applied
        property.parseValueFromImport(entity, Value.EMPTY)
        then:
        entity.getPrimitiveInt() == 0
    }

    def "primitive number fields get automatic default value from their initial assigned value"() {
        given:
        def property = mixing.getDescriptor(MongoDefaultValuesEntity.class).findProperty("primitiveIntWithValue")
        and:
        MongoDefaultValuesEntity entity = new MongoDefaultValuesEntity()
        entity.setPrimitiveIntWithValue(12)
        when: // an empty value is given, its default (50) should be applied
        property.parseValueFromImport(entity, Value.EMPTY)
        then:
        entity.getPrimitiveIntWithValue() == 50
    }

    def "amount fields which are not initialized should reset to Amount.NOTHING"() {
        given:
        def property = mixing.getDescriptor(MongoDefaultValuesEntity.class).findProperty("amount")
        and:
        MongoDefaultValuesEntity entity = new MongoDefaultValuesEntity()
        entity.setAmount(Amount.of(12))
        when:
        property.parseValueFromImport(entity, Value.EMPTY)
        then:
        entity.getAmount()== Amount.NOTHING
    }

    def "amount fields which are initialized with Amount.NOTHING should reset to Amount.NOTHING"() {
        given:
        def property = mixing.getDescriptor(MongoDefaultValuesEntity.class).findProperty("amountNothing")
        and:
        MongoDefaultValuesEntity entity = new MongoDefaultValuesEntity()
        entity.setAmountNothing(Amount.of(12))
        when:
        property.parseValueFromImport(entity, Value.EMPTY)
        then:
        entity.getAmountNothing()== Amount.NOTHING
    }
}
