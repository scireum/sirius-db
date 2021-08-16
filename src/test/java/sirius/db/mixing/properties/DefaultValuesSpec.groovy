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
        property.parseValueFromImport(entity, Value.of(""))
        then:
        entity.isPrimitiveBoolean()
    }
}
