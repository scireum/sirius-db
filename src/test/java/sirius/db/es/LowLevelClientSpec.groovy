/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es

import com.alibaba.fastjson.JSONObject
import sirius.db.mixing.Mixing
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part
import sirius.kernel.health.HandledException

class LowLevelClientSpec extends BaseSpecification {

    @Part
    private static Elastic elastic

    def "create index works"() {
        when:
        JSONObject obj = elastic.getLowLevelClient().createIndex("test", 1, 1)
        then:
        obj.acknowledged
    }

    def "error handling works"() {
        when:
        JSONObject obj = elastic.getLowLevelClient().createIndex("invalid", 0, 1)
        then:
        thrown(HandledException)
    }

    def "index / get / delete works"() {
        setup:
        elastic.getLowLevelClient().createIndex("test1", 1, 1)
        when:
        elastic.getLowLevelClient().index("test1", "lltest","TEST", null, null, new JSONObject().fluentPut("Hello", "World"))
        then:
        def data = elastic.getLowLevelClient().get("test1","lltest","TEST", null, true)
        and:
        data.found
        data._source.Hello == 'World'

        when:
        elastic.getLowLevelClient().delete("test1","lltest","TEST",null, null)
        and:
        data = elastic.getLowLevelClient().get("test1","lltest","TEST", null, true)
        then:
        !data.found
    }

}
