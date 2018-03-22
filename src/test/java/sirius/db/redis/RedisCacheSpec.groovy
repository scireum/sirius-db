/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.redis

import sirius.kernel.BaseSpecification
import sirius.kernel.cache.Cache
import sirius.kernel.cache.ValueComputer

import java.util.function.Predicate

class RedisCacheSpec extends BaseSpecification {

    // object under test
    Cache<String, String> cache

    def setup() {
        cache = new RedisCache("test-cache", null, null)
    }

    def cleanup() {
        cache.clear()
    }

    def "test adding and getting of a key"() {
        when:
        cache.put("key", "value")
        then:
        cache.get("key") == "value"
    }

    def "calculate value if it does not exist"() {
        when:
        cache.put("key1", "value1")
        then:
        cache.get("key1", { key -> "calculated-value1" } as ValueComputer) == "value1"
        cache.get("key2", { key -> "calculated-value2" } as ValueComputer) == "calculated-value2"
        // on second call, the calculated value of first call shoud be returned
        cache.get("key2", { key -> "calculated-value3" } as ValueComputer) == "calculated-value2"
    }

    def "test size"() {
        given:
        cache.put("key1", "")
        cache.put("key1", "")
        cache.put("key2", "")
        cache.put("key3", "")
        when:
        def size = cache.getSize()
        then:
        size == 3
    }

    def "test remove"() {
        when:
        cache.put("key", "value")
        then:
        cache.get("key") != null
        cache.remove("key")
        cache.get("key") == null
    }

    def "test contains"() {
        when:
        cache.put("key", "value")
        then:
        cache.contains("key") == true
        cache.contains("key2") == false
    }

    def "test keySet"() {
        when:
        cache.put("key1", "value")
        cache.put("key2", "value")
        cache.put("key3", "value")
        cache.put("key3", "value")
        Iterator<String> keys = cache.keySet()
        then:
        keys.next() == "key1"
        keys.next() == "key2"
        keys.next() == "key3"
        keys.hasNext() == false
    }

    def "test clear"() {
        when:
        cache.put("key1", "value")
        cache.put("key2", "value")
        cache.put("key3", "value")
        then:
        cache.getSize() == 3
        cache.clear()
        cache.getSize() == 0
    }

    def "test removeIf"() {
        when:
        cache.put("key1-a", "value")
        cache.put("key2-a", "value")
        cache.put("key3-b", "value")
        cache.removeIf({entry -> entry.getKey().contains("a")} as Predicate)
        then:
        cache.getSize() == 1
        cache.contains("key3-b")
        !cache.contains("key1-a")
        !cache.contains("key2-a")
    }
}
