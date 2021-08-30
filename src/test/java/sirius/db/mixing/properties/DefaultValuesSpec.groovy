/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties

import sirius.db.jdbc.DataTypesEntity
import sirius.db.mixing.Mixing
import sirius.db.mixing.SQLDefaultValuesEntity
import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Amount
import sirius.kernel.commons.Value
import sirius.kernel.di.std.Part

class DefaultValuesSpec extends BaseSpecification {

    @Part
    private static Mixing mixing

    def "the default values are properly initialized"() {
        expect:
        mixing.getDescriptor(SQLDefaultValuesEntity.class).findProperty(propertyName).getDefaultValue() ==
                expectedDefault

        where:
        propertyName            | expectedDefault
        "primitiveBoolean"      | Value.of(false)
        "primitiveBooleanTrue"  | Value.of(true)
        "booleanObject"         | Value.EMPTY
        "primitiveInt"          | Value.of(0)
        "primitiveIntWithValue" | Value.of(50)
        "amountWithValue"       | Value.of(Amount.ONE_HUNDRED)
        "amountZero"            | Value.of(Amount.ZERO)
        "amountNothing"         | Value.EMPTY //Amount.NOTHING should be considered null and therefore have no default
        "string"                | Value.EMPTY
        "emptyString"           | Value.of("")
        "enumWithValue"         | Value.of(DataTypesEntity.TestEnum.Test2)
    }

    def "column default values are properly transformed"() {
        expect:
        mixing.getDescriptor(SQLDefaultValuesEntity.class).findProperty(propertyName).getColumnDefaultValue() ==
                expectedDefault

        where:
        propertyName            | expectedDefault
        "primitiveBoolean"      | "0"
        "primitiveBooleanTrue"  | "1"
        "booleanObject"         | null
        "primitiveInt"          | "0"
        "primitiveIntWithValue" | "50"
        "amountWithValue"       | "100.000"
        "amountZero"            | "0.000"
        "amountNothing"         | null
        "string"                | null
        "emptyString"           | ""
        "filledString"          | "STRING"
        "enumTest"              | null
        "enumWithValue"         | "Test2"
    }

    def "a primitive boolean with no default value annotation does not throw an error"() {
        given:
        def property = mixing.getDescriptor(SQLDefaultValuesEntity.class).findProperty("primitiveBoolean")
        and:
        SQLDefaultValuesEntity entity = new SQLDefaultValuesEntity()
        entity.setPrimitiveBoolean(true)
        when: // an empty value is given, its default (false) should be applied
        property.parseValueFromImport(entity, Value.EMPTY)
        then:
        !entity.isPrimitiveBoolean()
    }

    def "a primitive boolean gets its default value from its initial assigned value"() {
        given:
        def property = mixing.getDescriptor(SQLDefaultValuesEntity.class).findProperty("primitiveBooleanTrue")
        and:
        SQLDefaultValuesEntity entity = new SQLDefaultValuesEntity()
        when: // an empty value is given, its default (true) should be applied
        property.parseValueFromImport(entity, Value.EMPTY)
        then:
        entity.isPrimitiveBooleanTrue()
    }

    def "a boolean object field can have no default value"() {
        given:
        def property = mixing.getDescriptor(SQLDefaultValuesEntity.class).findProperty("booleanObject")
        and:
        SQLDefaultValuesEntity entity = new SQLDefaultValuesEntity()
        entity.setBooleanObject(Boolean.TRUE)
        when: // an empty value is given, the field should reset to 'null'
        property.parseValueFromImport(entity, Value.EMPTY)
        then:
        entity.getBooleanObject() == null
    }

    def "primitive number fields get automatic default value from their initial default value"() {
        given:
        def property = mixing.getDescriptor(SQLDefaultValuesEntity.class).findProperty("primitiveInt")
        and:
        SQLDefaultValuesEntity entity = new SQLDefaultValuesEntity()
        entity.setPrimitiveInt(12)
        when: // an empty value is given, its default (0) should be applied
        property.parseValueFromImport(entity, Value.EMPTY)
        then:
        entity.getPrimitiveInt() == 0
    }

    def "primitive number fields get automatic default value from their initial assigned value"() {
        given:
        def property = mixing.getDescriptor(SQLDefaultValuesEntity.class).findProperty("primitiveIntWithValue")
        and:
        SQLDefaultValuesEntity entity = new SQLDefaultValuesEntity()
        entity.setPrimitiveIntWithValue(12)
        when: // an empty value is given, its default (50) should be applied
        property.parseValueFromImport(entity, Value.EMPTY)
        then:
        entity.getPrimitiveIntWithValue() == 50
    }

    def "amount fields which are initialized with Amount.NOTHING should reset to Amount.NOTHING"() {
        given:
        def property = mixing.getDescriptor(SQLDefaultValuesEntity.class).findProperty("amountNothing")
        and:
        SQLDefaultValuesEntity entity = new SQLDefaultValuesEntity()
        entity.setAmountNothing(Amount.of(12))
        when:
        property.parseValueFromImport(entity, Value.EMPTY)
        then:
        entity.getAmountNothing() == Amount.NOTHING
    }
}
