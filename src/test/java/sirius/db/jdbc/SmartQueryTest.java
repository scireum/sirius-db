package sirius.db.jdbc;

import org.junit.jupiter.api.Test;
import sirius.db.jdbc.constraints.SQLConstraint;
import sirius.db.mixing.Mapping;
import sirius.kernel.commons.Strings;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests the {@link SmartQuery} class by validating SQL expressions in aggregation fields and group-by clauses.
 */
public class SmartQueryTest {

    @Test
    public void testAggregationField_withNumericTestEntityFieldAndAlias_acceptsExpression() {
        SmartQuery<SmartQueryTestLargeTableEntity> query = new SmartQuery<>(null, null);

        query.aggregationField(Strings.apply("SUM(%s) as totalValue",
                                             SmartQueryTestLargeTableEntity.TEST_NUMBER.getName()));

        assertEquals("SUM(testNumber) as totalValue", query.aggregationFields.getFirst());
    }

    @Test
    public void testGroupBy_withStringFunction_acceptsExpression() {
        SmartQuery<SmartQueryTestSortingEntity> query = new SmartQuery<>(null, null);

        query.groupBy(Strings.apply("LOWER(%s)", SmartQueryTestSortingEntity.VALUE_ONE.getName()));

        assertEquals("LOWER(valueOne)", query.groupBys.getFirst());
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
        assertEquals("((valueOne = value1 AND valueTwo = value2) AND id > 1)", constraint.toString());
    }

    @Test
    public void testCreateSqlConstraintForSortingColumn_withoutPreviousColumns_returnValidSqlQuery() {
        SmartQuery<SmartQueryTestSortingEntity> query = new SmartQuery<>(null, null);
        Map<Mapping, Object> previousSortingColumns = new HashMap<>();
        SQLConstraint constraint = query.createSqlConstraintForSortingColumn(true,
                SmartQueryTestSortingEntity.ID,
                "1",
                previousSortingColumns);
        assertEquals("id > 1", constraint.toString());
    }

    @Test
    public void testAggregationField_withForbiddenKeywordInsideStringLiteral_acceptsExpression() {
        SmartQuery<SmartQueryTestSortingEntity> query = new SmartQuery<>(null, null);

        query.aggregationField("countIf(valueOne = 'EXECUTE-FOOBAR') as filteredCount");

        assertEquals("countIf(valueOne = 'EXECUTE-FOOBAR') as filteredCount", query.aggregationFields.getFirst());
    }

    @Test
    public void testAggregationField_withUnterminatedStringLiteral_rejectsExpression() {
        assertInvalidSQLExpression(() -> new SmartQuery<SmartQueryTestSortingEntity>(null, null)
                .aggregationField("countIf(valueOne = 'unterminated)"));
    }

    @Test
    public void testAggregationField_withForbiddenKeywordOutsideStringLiteral_rejectsExpression() {
        assertInvalidSQLExpression(() -> new SmartQuery<SmartQueryTestSortingEntity>(null, null)
                .aggregationField("countIf(valueOne = 'safe') UNION SELECT id"));
    }

    @Test
    public void testAggregationField_withBackslashEscapedQuoteHidingKeyword_rejectsExpression() {
        // A backslash must not be treated as an escape for the closing quote, otherwise the trailing UNION SELECT
        // (which is real SQL in dialects with standard string handling) would be hidden inside the literal.
        assertInvalidSQLExpression(() -> new SmartQuery<SmartQueryTestSortingEntity>(null, null)
                .aggregationField("countIf(valueOne = 'abc\\') UNION SELECT id"));
    }

    private void assertInvalidSQLExpression(Runnable runnable) {
        try {
            runnable.run();
            fail("Expected an IllegalArgumentException.");
        } catch (IllegalArgumentException exception) {
            assertNotNull(exception.getMessage());
        }
    }
}
