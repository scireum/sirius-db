/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.jdbc.batch

import sirius.db.jdbc.OMA
import sirius.db.jdbc.TestEntity
import sirius.kernel.BaseSpecification
import sirius.kernel.di.std.Part

import java.time.Duration

class BatchContextSpec extends BaseSpecification {

    @Part
    private static OMA oma

    def setupSpec() {
        oma.getReadyFuture().await(Duration.ofSeconds(60))
    }

    def "insert works"() {
        setup:
        BatchContext ctx = new BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        when:
        InsertQuery<TestEntity> insert = ctx.insertQuery(
                TestEntity.class,
                TestEntity.FIRSTNAME,
                TestEntity.LASTNAME,
                TestEntity.AGE)
        and:
        for (int i = 0; i < 100; i++) {
            TestEntity e = new TestEntity()
            e.setFirstname("BatchContextInsert" + i)
            e.setLastname("INSERT")
            insert.insert(e, true, false)
        }
        then:
        oma.select(TestEntity.class).eq(TestEntity.LASTNAME, "INSERT").count() == 100
        cleanup:
        OMA.LOG.INFO(ctx)
        ctx.close()
    }

    def "batch insert works"() {
        setup:
        BatchContext ctx = new BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        when:
        InsertQuery<TestEntity> insert = ctx.insertQuery(
                TestEntity.class,
                TestEntity.FIRSTNAME,
                TestEntity.LASTNAME,
                TestEntity.AGE)
        and:
        for (int i = 0; i < 100; i++) {
            TestEntity e = new TestEntity()
            e.setFirstname("BatchContextInsert" + i)
            e.setLastname("BATCHINSERT")
            insert.insert(e, false, true)
        }
        and:
        then:
        oma.select(TestEntity.class).eq(TestEntity.LASTNAME, "BATCHINSERT").count() == 0
        and:
        insert.commit()
        oma.select(TestEntity.class).eq(TestEntity.LASTNAME, "BATCHINSERT").count() == 100
        cleanup:
        OMA.LOG.INFO(ctx)
        ctx.close()
    }

    def "update works"() {
        setup:
        TestEntity e = new TestEntity()
        e.setFirstname("Updating")
        e.setLastname("Test")
        and:
        oma.update(e)
        and:
        BatchContext ctx = new BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        when:
        UpdateQuery<TestEntity> update = ctx.updateByIdQuery(TestEntity.class, TestEntity.FIRSTNAME)
        and:
        e.setFirstname("Updated")
        update.update(e, true, false)
        then:
        oma.refreshOrFail(e).getFirstname() == "Updated"
        cleanup:
        OMA.LOG.INFO(ctx)
        ctx.close()
    }

    def "batch update works"() {
        setup:
        TestEntity e = new TestEntity()
        e.setFirstname("Updating")
        e.setLastname("Test")
        and:
        oma.update(e)
        and:
        BatchContext ctx = new BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        when:
        UpdateQuery<TestEntity> update = ctx.updateByIdQuery(TestEntity.class, TestEntity.FIRSTNAME)
        and:
        e.setFirstname("Updated")
        update.update(e, true, true)
        then:
        oma.refreshOrFail(e).getFirstname() != "Updated"
        and:
        update.commit()
        oma.refreshOrFail(e).getFirstname() == "Updated"
        cleanup:
        OMA.LOG.INFO(ctx)
        ctx.close()
    }

    def "find works"() {
        setup:
        TestEntity e = new TestEntity()
        e.setFirstname("BatchContextFind")
        e.setLastname("Test")
        and:
        oma.update(e)
        and:
        BatchContext ctx = new BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        when:
        FindQuery<TestEntity> find = ctx.findQuery(TestEntity.class, TestEntity.FIRSTNAME)
        and:
        TestEntity found = new TestEntity()
        found.setFirstname("BatchContextFind")
        found.setLastname("Test")
        and:
        TestEntity notFound = new TestEntity()
        notFound.setFirstname("BatchContextFind2")
        found.setLastname("Test")
        then:
        find.find(found) == e
        and:
        !find.find(found).isNew()
        and:
        find.find(notFound).isNew()
        cleanup:
        OMA.LOG.INFO(ctx)
        ctx.close()
    }


    def "delete works"() {
        setup:
        TestEntity e = new TestEntity()
        e.setFirstname("Delete")
        e.setLastname("Test")
        and:
        oma.update(e)
        and:
        BatchContext ctx = new BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        when:
        DeleteQuery<TestEntity> delete = ctx.deleteQuery(TestEntity.class, TestEntity.FIRSTNAME)
        and:
        delete.delete(e, true, false)
        then:
        !oma.resolve(e.getUniqueName()).isPresent()
        cleanup:
        OMA.LOG.INFO(ctx)
        ctx.close()
    }

    def "delete update works"() {
        setup:
        TestEntity e = new TestEntity()
        e.setFirstname("BatchDelete")
        e.setLastname("Test")
        and:
        oma.update(e)
        and:
        BatchContext ctx = new BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        when:
        DeleteQuery<TestEntity> delete = ctx.deleteQuery(TestEntity.class, TestEntity.FIRSTNAME)
        and:
        delete.delete(e, true, true)
        then:
        oma.resolve(e.getUniqueName()).isPresent()
        and:
        delete.commit()
        !oma.resolve(e.getUniqueName()).isPresent()
        cleanup:
        OMA.LOG.INFO(ctx)
        ctx.close()
    }

    def "custom query works"() {
        setup:
        TestEntity e = new TestEntity()
        e.setFirstname("BatchContextFind")
        e.setLastname("CustomTest")
        and:
        oma.update(e)
        BatchContext ctx = new BatchContext({ -> "Test" }, Duration.ofMinutes(2))
        when:
        CustomQuery find = ctx.customQuery(TestEntity.class, false, "SELECT * FROM testentity WHERE lastname = ?")
        then:
        find.setParameter(1, "CustomTest")
        find.query().queryFirst().getValue("LASTNAME").asString() == "CustomTest"
        and:
        find.setParameter(1, "CustomTestXXX")
        !find.query().first().isPresent()
        cleanup:
        OMA.LOG.INFO(ctx)
        ctx.close()
    }

}
