/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mixing.properties

import sirius.db.es.properties.ESDataTypesEntity
import sirius.db.mixing.Mixing
import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Value
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException

class EnumPropertySpec extends BaseSpecification {

    @Part
    private static Mixing mixing

    def "resolving via enum constants and translations works"() {
        given:
        def property = mixing.getDescriptor(ESDataTypesEntity.class).findProperty("enumValue")
        and:
        ESDataTypesEntity entity = new ESDataTypesEntity();
        when: // An enum constant can be resolved by its name...
        property.parseValueFromImport(entity, Value.of("Test1"))
        then:
        entity.getTestEnum() == ESDataTypesEntity.TestEnum.Test1

        when: // An enum constant can be resolved by its name in lowercase...
        property.parseValueFromImport(entity, Value.of("test1"))
        then:
        entity.getTestEnum() == ESDataTypesEntity.TestEnum.Test1

        when: // Invalid values throws an exception
        property.parseValueFromImport(entity, Value.of("test0"))
        then:
        thrown(HandledException)
        when: // Enum constants can be resolved by their "toString" representation...
        property.parseValueFromImport(entity, Value.of("test25"))
        then:
        entity.getTestEnum() == ESDataTypesEntity.TestEnum.Test2

    }

}
