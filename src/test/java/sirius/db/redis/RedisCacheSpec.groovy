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
import sirius.kernel.cache.CacheEntry
import sirius.kernel.cache.ValueComputer
import sirius.kernel.cache.ValueVerifier
import sirius.kernel.commons.Callback
import sirius.kernel.commons.Monoflop
import sirius.kernel.commons.Tuple
import sirius.kernel.commons.ValueHolder
import sirius.kernel.commons.Wait

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
        cache.removeIf({ entry -> entry.getKey().contains("a") } as Predicate)
        then:
        cache.getSize() == 1
        cache.contains("key3-b")
        !cache.contains("key1-a")
        !cache.contains("key2-a")
    }

    def "test getContents"() {
        when:
        cache.put("key1", "value1")
        cache.put("key2", "value2")
        cache.put("key3", "value3")
        List<CacheEntry<String, String>> entries = cache.getContents()
        then:
        entries.size() == 3
        entries.get(0).getKey() == "key1"
        entries.get(0).getValue() == "value1"
        entries.get(1).getKey() == "key2"
        entries.get(1).getValue() == "value2"
        entries.get(2).getKey() == "key3"
        entries.get(2).getValue() == "value3"
    }

    def "test hitrate"() {
        when:
        cache.put("key1", "value")
        cache.get("key1")
        cache.get("key2")
        then:
        cache.getHitRate() == 50
    }

    def "test hitrate returns 0 if no call was made"() {
        when:
        def hitrate = cache.getHitRate()
        then:
        hitrate == 0
    }

    def "test hitrate history"() {
        when:
        cache.put("key1", "value")
        cache.get("key1")
        cache.get("key2")
        cache.updateStatistics()
        cache.get("key1")
        cache.get("key2")
        cache.get("key2")
        cache.updateStatistics()
        then:
        cache.getHitRate() == 0
        and:
        cache.getHitRateHistory().size() == 2
        cache.getHitRateHistory().get(0) == 50
        cache.getHitRateHistory().get(1) == 33
    }

    def "tets getUses"() {
        when:
        cache.put("key1", "value")
        cache.get("key1")
        cache.get("key2")
        then:
        cache.getUses() == 2
    }

    def "test getUses history"() {
        when:
        cache.put("key1", "value")
        cache.get("key1")
        cache.get("key2")
        cache.updateStatistics()
        cache.get("key1")
        cache.get("key2")
        cache.get("key2")
        cache.updateStatistics()
        then:
        cache.getUses() == 0
        and:
        cache.getUseHistory().size() == 2
        cache.getUseHistory().get(0) == 2
        cache.getUseHistory().get(1) == 3
    }

    def "test onRemove-Callback is called after remove"() {
        given:
        Monoflop onRemoveCalled = Monoflop.create()
        cache.onRemove({ tuple -> onRemoveCalled.toggle() } as Callback)
        when:
        cache.put("key", "value")
        then:
        onRemoveCalled.isToggled() == false
        cache.remove("not-existing-key")
        onRemoveCalled.isToggled() == false
        cache.remove("key")
        onRemoveCalled.isToggled() == true
    }

    def "test onRemove-Callback is called with right values"() {
        given:
        ValueHolder<Tuple<String, String>> resultTuple = new ValueHolder()
        cache.onRemove({ tuple -> resultTuple.set(tuple) } as Callback)
        when:
        cache.put("key", "value")
        cache.remove("key")
        then:
        resultTuple.get().getFirst() == "key"
        resultTuple.get().getSecond() == "value"
    }

    def "test evict afer ttl is reached"() {
        when:
        cache.put("key", "value")
        then:
        cache.get("key") == "value"
        and:
        Wait.seconds(6)
        cache.get("key") == null
    }

    def "test call verify when it is time, remove value if verifier returns false"() {
        given:
        Monoflop verifierCalled = Monoflop.create()
        cache = new RedisCache("test-cache", null, { value -> verifierCalled.toggle() } as ValueVerifier)
        when:
        cache.put("key", "value")
        cache.get("key")
        then:
        verifierCalled.isToggled() == false
        and:
        Wait.seconds(3)
        cache.get("key") == null
        verifierCalled.isToggled() == true
    }

    def "test call verify when it is time, keep value if verifier returns true"() {
        given:
        Monoflop verifierCalled = Monoflop.create()
        cache = new RedisCache("test-cache", null, { value -> !verifierCalled.toggle() } as ValueVerifier)
        when:
        cache.put("key", "value")
        cache.get("key")
        then:
        verifierCalled.isToggled() == false
        and:
        Wait.seconds(3)
        cache.get("key") == "value"
        verifierCalled.isToggled() == true
    }

    def "test runEviction"() {
        when:
        cache.put("key1", "value1")
        cache.put("key2", "value2")
        Wait.seconds(6)
        cache.put("key3", "value3")
        cache.put("key4", "value4")
        then:
        cache.getSize() == 4
        cache.runEviction()
        cache.getSize() == 2
        def entries = cache.getContents()
        entries.size() == 2
        entries.get(0).getKey() == "key3"
        entries.get(0).getValue() == "value3"
        entries.get(1).getKey() == "key4"
        entries.get(1).getValue() == "value4"
    }
}
