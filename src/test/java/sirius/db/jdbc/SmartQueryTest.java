package sirius.db.jdbc;

import org.junit.Assert;
import org.junit.Test;
import sirius.db.jdbc.constraints.SQLConstraint;
import sirius.db.mixing.Mapping;
import sirius.kernel.commons.Strings;

import java.util.HashMap;
import java.util.Map;

public class SmartQueryTest {

    @Test
    public void testAggregationField_withNumericTestEntityFieldAndAlias_acceptsExpression() {
        SmartQuery<SmartQueryTestLargeTableEntity> query = new SmartQuery<>(null, null);

        query.aggregationField(Strings.apply("SUM(%s) as totalValue",
                                             SmartQueryTestLargeTableEntity.TEST_NUMBER.getName()));

        Assert.assertEquals("SUM(testNumber) as totalValue", query.aggregationFields.getFirst());
    }

    @Test
    public void testGroupBy_withStringFunction_acceptsExpression() {
        SmartQuery<SmartQueryTestSortingEntity> query = new SmartQuery<>(null, null);

        query.groupBy(Strings.apply("LOWER(%s)", SmartQueryTestSortingEntity.VALUE_ONE.getName()));

        Assert.assertEquals("LOWER(valueOne)", query.groupBys.getFirst());
    }

    @Test
    public void testAggregationField_withAdditionalStatement_rejectsExpression() {
        assertInvalidSQLExpression(() -> new SmartQuery<SmartQueryTestSortingEntity>(null, null)
                .aggregationField("COUNT(*); DROP TABLE smartquerytestsortingentity; --"));
    }

    @Test
    public void testAggregationField_withSelectKeyword_rejectsExpression() {
        assertInvalidSQLExpression(() -> new SmartQuery<SmartQueryTestSortingEntity>(null, null)
                .aggregationField("(SELECT id)"));
    }

    @Test
    public void testGroupBy_withSubqueryKeyword_rejectsExpression() {
        assertInvalidSQLExpression(() -> new SmartQuery<SmartQueryTestSortingEntity>(null, null)
                .groupBy("LOWER(valueOne) FROM smartquerytestsortingentity"));
    }

    @Test
    public void testCreateSqlConstraintForSortingColumn_withPreviousColumns_returnValidSqlQuery() {
        SmartQuery<SmartQueryTestSortingEntity> query = new SmartQuery<>(null, null);
        Map<Mapping, Object> previousSortingColumns = new HashMap<>();
        previousSortingColumns.put(SmartQueryTestSortingEntity.VALUE_ONE, "value1");
        previousSortingColumns.put(SmartQueryTestSortingEntity.VALUE_TWO, "value2");
        SQLConstraint constraint = query.createSqlConstraintForSortingColumn(true,
                SmartQueryTestSortingEntity.ID,
                "1",
                previousSortingColumns);
        Assert.assertEquals("((valueOne = value1 AND valueTwo = value2) AND id > 1)", constraint.toString());
    }

    @Test
    public void testCreateSqlConstraintForSortingColumn_withoutPreviousColumns_returnValidSqlQuery() {
        SmartQuery<SmartQueryTestSortingEntity> query = new SmartQuery<>(null, null);
        Map<Mapping, Object> previousSortingColumns = new HashMap<>();
        SQLConstraint constraint = query.createSqlConstraintForSortingColumn(true,
                SmartQueryTestSortingEntity.ID,
                "1",
                previousSortingColumns);
        Assert.assertEquals("id > 1", constraint.toString());
    }

    private void assertInvalidSQLExpression(Runnable runnable) {
        try {
            runnable.run();
            Assert.fail("Expected an IllegalArgumentException.");
        } catch (IllegalArgumentException exception) {
            Assert.assertNotNull(exception.getMessage());
        }
    }
}
