package sirius.db.jdbc;

import sirius.kernel.BaseSpecification;
import sirius.kernel.di.std.Part;

class SmartQuerySortingSpec extends BaseSpecification {
    @Part
    static OMA oma;

    private static final int TWO_BLOCKS_LENGTH = 2000
    private static final int ONE_AND_HALF_BLOCK_LENGTH = 1500
    private static final int ONE_BLOCK_LENGTH = 1000
    private static final int FIRST_COL = 0;
    private static final int SECOND_COL = 1;
    private static final int NO_COL = -1;

    def cleanup() {
        oma.select(SmartQueryTestSortingEntity.class).delete()
    }

    def "sorting asc without null values returns all entries"() {
        when:
        prepareWithNonNullValues()
        SmartQuery<SmartQueryTestSortingEntity> qry = createAscSortingQuery()
        then:
        isCorrectlySortedAsc(qry)
    }

    def "sorting desc without null values returns all entries"() {
        when:
        prepareWithNonNullValues()
        SmartQuery<SmartQueryTestSortingEntity> qry = createDescSortingQuery()
        then:
        isCorrectlySortedDesc(qry)
    }

    def "sorting asc with null values for first column of the first 1.5 blocks returns all entries"() {
        when:
        prepareWithNullValues(ONE_AND_HALF_BLOCK_LENGTH, FIRST_COL);
        SmartQuery<SmartQueryTestSortingEntity> qry = createAscSortingQuery()
        then:
        isCorrectlySortedAsc(qry)
    }

    def "sorting asc with null values for second column of the first 1.5 blocks returns all entries"() {
        when:
        prepareWithNullValues(ONE_AND_HALF_BLOCK_LENGTH, SECOND_COL);
        SmartQuery<SmartQueryTestSortingEntity> qry = createAscSortingQuery()
        then:
        isCorrectlySortedAsc(qry)
    }

    def "sorting desc with null values for first column of the first 1.5 blocks returns all entries"() {
        when:
        prepareWithNullValues(ONE_AND_HALF_BLOCK_LENGTH, FIRST_COL);
        SmartQuery<SmartQueryTestSortingEntity> qry = createDescSortingQuery()
        then:
        isCorrectlySortedDesc(qry)
    }

    def "sorting desc with null values for second column of the first 1.5 blocks returns all entries"() {
        when:
        prepareWithNullValues(ONE_AND_HALF_BLOCK_LENGTH, SECOND_COL);
        SmartQuery<SmartQueryTestSortingEntity> qry = createDescSortingQuery()
        then:
        isCorrectlySortedDesc(qry)
    }

    def "sorting asc with null values for first column of the first block returns all entries"() {
        when:
        prepareWithNullValues(ONE_BLOCK_LENGTH, FIRST_COL);
        SmartQuery<SmartQueryTestSortingEntity> qry = createAscSortingQuery()
        then:
        isCorrectlySortedAsc(qry)
    }

    def "sorting desc with null values for second column of the first block returns all entries"() {
        when:
        prepareWithNullValues(ONE_BLOCK_LENGTH, SECOND_COL);
        SmartQuery<SmartQueryTestSortingEntity> qry = createDescSortingQuery()
        then:
        isCorrectlySortedDesc(qry)
    }

    def "sorting asc with null values for first column of the first block-1 entries returns all entries"() {
        when:
        prepareWithNullValues(ONE_BLOCK_LENGTH-1, FIRST_COL);
        SmartQuery<SmartQueryTestSortingEntity> qry = createAscSortingQuery()
        then:
        isCorrectlySortedAsc(qry)
    }

    def "sorting desc with null values for first column of the first block-1 entries returns all entries"() {
        when:
        prepareWithNullValues(ONE_BLOCK_LENGTH-1, FIRST_COL);
        SmartQuery<SmartQueryTestSortingEntity> qry = createDescSortingQuery()
        then:
        isCorrectlySortedDesc(qry)
    }

    private void prepareWithNonNullValues() {
        prepareWithNullValues(0, NO_COL)
    }

    private void prepareWithNullValues(int amountOfNullValues, int column) {
        for (int i = 1; i <= TWO_BLOCKS_LENGTH; i++) {
        SmartQueryTestSortingEntity e = new SmartQueryTestSortingEntity()
        if (i > amountOfNullValues) {
            e.setValueOne("valueOne_" + generateMixedInt(i))
            e.setValueTwo("valueTwo_" + generateMixedInt(i))
        } else {
            switch (column) {
                case FIRST_COL:
                e.setValueTwo("valueTwo_" + generateMixedInt(i))
                break;
                case SECOND_COL:
                e.setValueOne("valueOne_" + generateMixedInt(i))
                break;
                default:
                e.setValueOne("valueOne_" + generateMixedInt(i))
                e.setValueTwo("valueTwo_" + generateMixedInt(i))
            }
        }
        oma.update(e)
    }
    }

    private SmartQuery<SmartQueryTestSortingEntity> createAscSortingQuery() {
        oma.select(SmartQueryTestSortingEntity.class)
                .orderAsc(SmartQueryTestSortingEntity.VALUE_ONE)
                .orderAsc(SmartQueryTestSortingEntity.VALUE_TWO)
    }

    private SmartQuery<SmartQueryTestSortingEntity> createDescSortingQuery() {
        oma.select(SmartQueryTestSortingEntity.class)
                .orderDesc(SmartQueryTestSortingEntity.VALUE_ONE)
                .orderDesc(SmartQueryTestSortingEntity.VALUE_TWO)
    }

    private boolean isCorrectlySortedAsc(SmartQuery<SmartQueryTestSortingEntity> query) {
        return query.count() == TWO_BLOCKS_LENGTH &&
                query.streamBlockwise().toList().first().getId() == createAscSortingQuery().queryFirst().getId() &&
                query.streamBlockwise().toList().last().getId() == createDescSortingQuery().queryFirst().getId()
    }

    private boolean isCorrectlySortedDesc(SmartQuery<SmartQueryTestSortingEntity> query) {
        return query.count() == TWO_BLOCKS_LENGTH &&
                query.streamBlockwise().toList().first().getId() == createDescSortingQuery().queryFirst().getId() &&
                query.streamBlockwise().toList().last().getId() == createAscSortingQuery().queryFirst().getId();
    }

    /**
     * Based on the index this method generates an Integer that can be used to simulate an unordered list.
     * </p>
     * If it's used in a for-loop with index e.g. 1,2,3,4 it returns 1,4,3,6,5,....
     *
     * @param index the current index of the for loop
     * @return int based on the index
     */
    private int generateMixedInt(int index) {
        if (index % 2 == 0) {
            return index+2;
        }
        return index
    }
}
