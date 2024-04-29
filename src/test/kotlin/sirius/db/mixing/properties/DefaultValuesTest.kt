/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import sirius.db.jdbc.DataTypesEntity
import sirius.db.mixing.Mixing
import sirius.db.mixing.SQLDefaultValuesEntity
import sirius.kernel.SiriusExtension
import sirius.kernel.commons.Amount
import sirius.kernel.commons.Value
import sirius.kernel.di.std.Part
import java.util.stream.Stream
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class DefaultValuesTest {
    @ParameterizedTest
    @MethodSource("provideParametersForValuesInitialized")
    fun `the default values are properly initialized`(propertyName: String, expectedDefaultValue: Value) {
        assertEquals(
                expectedDefaultValue,
                mixing.getDescriptor(SQLDefaultValuesEntity::class.java).findProperty(propertyName)?.defaultValue
        )
    }

    @ParameterizedTest
    @MethodSource("provideParametersForValuesTransformed")
    fun `column default values are properly transformed`(propertyName: String, expectedDefault: String?) {
        assertEquals(
                mixing.getDescriptor(SQLDefaultValuesEntity::class.java).findProperty(propertyName)
                        ?.getColumnDefaultValue(),
                expectedDefault,
        )
    }

    @Test
    fun `a primitive boolean with no default value annotation does not throw an error`() {
        val property = mixing.getDescriptor(SQLDefaultValuesEntity::class.java).findProperty("primitiveBoolean")
        val sqlDefaultValuesEntity = SQLDefaultValuesEntity()
        sqlDefaultValuesEntity.isPrimitiveBoolean = true
        property?.parseValueFromImport(sqlDefaultValuesEntity, Value.EMPTY)

        assertFalse { sqlDefaultValuesEntity.isPrimitiveBoolean }
    }

    @Test
    fun `a primitive boolean gets its default value from its initial assigned value`() {
        val property = mixing.getDescriptor(SQLDefaultValuesEntity::class.java).findProperty("primitiveBooleanTrue")
        val sqlDefaultValuesEntity = SQLDefaultValuesEntity()
        property?.parseValueFromImport(sqlDefaultValuesEntity, Value.EMPTY)

        assertTrue { sqlDefaultValuesEntity.isPrimitiveBooleanTrue }
    }

    @Test
    fun `a boolean object field can have no default value`() {
        val property = mixing.getDescriptor(SQLDefaultValuesEntity::class.java).findProperty("booleanObject")
        val sqlDefaultValuesEntity = SQLDefaultValuesEntity()
        sqlDefaultValuesEntity.booleanObject = true
        property?.parseValueFromImport(sqlDefaultValuesEntity, Value.EMPTY)

        assertNull(sqlDefaultValuesEntity.booleanObject)
    }

    @Test
    fun `primitive number fields get automatic default value from their initial default value`() {
        val property = mixing.getDescriptor(SQLDefaultValuesEntity::class.java).findProperty("primitiveInt")
        val sqlDefaultValuesEntity = SQLDefaultValuesEntity()
        sqlDefaultValuesEntity.primitiveInt = 12
        property?.parseValueFromImport(sqlDefaultValuesEntity, Value.EMPTY)

        assertEquals(0, sqlDefaultValuesEntity.primitiveInt)
    }

    @Test
    fun `primitive number fields get automatic default value from their initial assigned value`() {
        val property = mixing.getDescriptor(SQLDefaultValuesEntity::class.java).findProperty("primitiveIntWithValue")
        val sqlDefaultValuesEntity = SQLDefaultValuesEntity()
        sqlDefaultValuesEntity.primitiveIntWithValue = 12
        property?.parseValueFromImport(sqlDefaultValuesEntity, Value.EMPTY)

        assertEquals(50, sqlDefaultValuesEntity.primitiveIntWithValue)
    }

    @Test
    fun `amount fields which are initialized with Amount#NOTHING should reset to Amount#NOTHING`() {
        val property = mixing.getDescriptor(SQLDefaultValuesEntity::class.java).findProperty("amountNothing")
        val sqlDefaultValuesEntity = SQLDefaultValuesEntity()
        val amount = 12
        sqlDefaultValuesEntity.amountNothing = Amount.of(amount)
        property?.parseValueFromImport(sqlDefaultValuesEntity, Value.EMPTY)

        assertEquals(Amount.NOTHING, sqlDefaultValuesEntity.amountNothing)
    }

    @ParameterizedTest
    @MethodSource("provideParametersForAmountInitialized")
    fun `amount fields initialized with a non-empty Amount should persist the default when emptied`(
            input: Value,
            output: Amount
    ) {
        val property = mixing.getDescriptor(SQLDefaultValuesEntity::class.java).findProperty("amountZero")
        val entity = SQLDefaultValuesEntity()
        val amountSeventySeven = 771
        entity.amountZero = Amount.of(amountSeventySeven)
        property?.parseValueFromImport(entity, input)

        assertEquals(output, entity.amountZero)
    }

    companion object {
        @Part
        private lateinit var mixing: Mixing

        @JvmStatic
        fun provideParametersForValuesInitialized(): Stream<Arguments> {
            return Stream.of(
                    Arguments.of("primitiveBoolean", Value.of(false)),
                    Arguments.of("primitiveBooleanTrue", Value.of(true)),
                    Arguments.of("booleanObject", Value.EMPTY),
                    Arguments.of("primitiveInt", Value.of(0)),
                    Arguments.of("primitiveIntWithValue", Value.of(50)),
                    Arguments.of("amountWithValue", Value.of(Amount.ONE_HUNDRED)),
                    Arguments.of("amountZero", Value.of(Amount.ZERO)),
                    Arguments.of(
                            "amountNothing",
                            Value.EMPTY
                    ), //Amount.NOTHING should be considered null and therefore have no default
                    Arguments.of("string", Value.EMPTY),
                    Arguments.of("emptyString", Value.of("")),
                    Arguments.of("enumWithValue", Value.of(DataTypesEntity.TestEnum.Test2)),
            )
        }

        @JvmStatic
        fun provideParametersForValuesTransformed(): Stream<Arguments> {
            return Stream.of(
                    Arguments.of("primitiveBoolean", "0"),
                    Arguments.of("primitiveBooleanTrue", "1"),
                    Arguments.of("booleanObject", null),
                    Arguments.of("primitiveInt", "0"),
                    Arguments.of("primitiveIntWithValue", "50"),
                    Arguments.of("amountWithValue", "100.000"),
                    Arguments.of("amountZero", "0.000"),
                    Arguments.of("amountNothing", null),
                    Arguments.of("string", null),
                    Arguments.of("emptyString", ""),
                    Arguments.of("filledString", "STRING"),
                    Arguments.of("enumTest", null),
                    Arguments.of("enumWithValue", "Test2"),
            )
        }

        @JvmStatic
        fun provideParametersForAmountInitialized(): Stream<Arguments> {
            val valueFive = 5
            return Stream.of(
                    Arguments.of(Value.EMPTY, Amount.ZERO),
                    Arguments.of(Value.of(5), Amount.of(valueFive)),
                    Arguments.of(Value.of(Amount.NOTHING), Amount.ZERO),
                    Arguments.of(Value.of(Amount.of(44.1)), Amount.of(44.1)),
            )
        }
    }
}
