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
import sirius.kernel.commons.Value
import sirius.kernel.di.std.Part

class DefaultValuesSpec extends BaseSpecification {

    @Part
    private static Mixing mixing

    def "default value works on primitive boolean"() {
        given:
        def property = mixing.getDescriptor(MongoDefaultValuesEntity.class).findProperty("primitiveBoolean")
        and:
        MongoDefaultValuesEntity entity = new MongoDefaultValuesEntity()
        when: // an empty value is given, its default (true) should be applied
        property.parseValueFromImport(entity, Value.EMPTY)
        then:
        entity.isPrimitiveBoolean()
    }

    def "primitive number fields get an automatic default value"() {
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

    def "primitive number fields default value annotation works"() {
        given:
        def property = mixing.getDescriptor(MongoDefaultValuesEntity.class).findProperty("primitiveIntWithDefault")
        and:
        MongoDefaultValuesEntity entity = new MongoDefaultValuesEntity()
        entity.setPrimitiveIntWithDefault(12)
        when: // an empty value is given, its default (50) should be applied
        property.parseValueFromImport(entity, Value.EMPTY)
        then:
        entity.getPrimitiveIntWithDefault() == 50
    }
}
