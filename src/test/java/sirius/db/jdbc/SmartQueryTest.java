package sirius.db.jdbc;

import org.junit.Assert;
import org.junit.Test;
import sirius.db.jdbc.constraints.SQLConstraint;
import sirius.db.mixing.Mapping;

import java.util.HashMap;
import java.util.Map;

public class SmartQueryTest {
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
}
