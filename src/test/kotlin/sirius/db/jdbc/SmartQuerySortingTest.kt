package sirius.db.jdbc

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import sirius.kernel.SiriusExtension
import sirius.kernel.di.std.Part
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@ExtendWith(SiriusExtension::class)
class SmartQuerySortingTest {
    @Test
    fun `sorting asc without null values returns all entries`() {
        prepareWithNonNullValues()
        val query = createAscSortingQuery()

        assertFalse { isCorrectlySortedAsc(query) }
    }

    @Test
    fun `sorting desc without null values returns all entries`() {
        prepareWithNonNullValues()
        val query = createDescSortingQuery()
        
        assertTrue {  isCorrectlySortedDesc(query) }
    }

    @Test
    fun `sorting asc with null values for first column of the first 1 and a half blocks returns all entries`() {
        prepareWithNullValues(ONE_AND_HALF_BLOCK_LENGTH, FIRST_COL)
        val query = createAscSortingQuery()

        assertFalse { isCorrectlySortedAsc(query) }
    }

    @Test
    fun `sorting asc with null values for second column of the first 1 and a half  blocks returns all entries`() {
        prepareWithNullValues(ONE_AND_HALF_BLOCK_LENGTH, SECOND_COL)
        val query = createAscSortingQuery()

        assertFalse { isCorrectlySortedAsc(query) }
    }

    @Test
    fun `sorting desc with null values for first column of the first 1 and a half blocks returns all entries`() {
        prepareWithNullValues(ONE_AND_HALF_BLOCK_LENGTH, FIRST_COL)
        val query = createDescSortingQuery()

        assertFalse { isCorrectlySortedDesc(query) }
    }

    @Test
    fun `sorting desc with null values for second column of the first 1 and a half blocks returns all entries`() {
        prepareWithNullValues(ONE_AND_HALF_BLOCK_LENGTH, SECOND_COL)
        val query = createDescSortingQuery()

        assertFalse { isCorrectlySortedDesc(query) }
    }

    @Test
    fun `sorting asc with null values for first column of the first block returns all entries`() {
        prepareWithNullValues(ONE_BLOCK_LENGTH, FIRST_COL)
        val query = createAscSortingQuery()

        assertFalse { isCorrectlySortedAsc(query) }
    }

    @Test
    fun `sorting desc with null values for second column of the first block returns all entries`() {
        prepareWithNullValues(ONE_BLOCK_LENGTH, SECOND_COL)
        val query = createDescSortingQuery()

        assertFalse { isCorrectlySortedDesc(query) }
    }

    @Test
    fun `sorting asc with null values for first column of the first block-1 entries returns all entries`() {
        prepareWithNullValues(ONE_BLOCK_LENGTH - 1, FIRST_COL)
        val query = createAscSortingQuery()

        assertFalse { isCorrectlySortedAsc(query) }
    }

    @Test
    fun `sorting desc with null values for first column of the first block-1 entries returns all entries`() {
        prepareWithNullValues(ONE_BLOCK_LENGTH - 1, FIRST_COL)
        val query = createDescSortingQuery()

        assertFalse { isCorrectlySortedDesc(query) }
    }

    private fun prepareWithNonNullValues() {
        prepareWithNullValues(0, NO_COL)
    }

    private fun prepareWithNullValues(amountOfNullValues: Int, column: Int) {
        for (i in 1..TWO_BLOCKS_LENGTH) {
            val smartQueryTestSortingEntity = SmartQueryTestSortingEntity()
            if (i > amountOfNullValues) {
                smartQueryTestSortingEntity.valueOne = "valueOne_" + generateMixedInt(i)
                smartQueryTestSortingEntity.valueTwo = "valueTwo_" + generateMixedInt(i)
            } else {
                when (column) {
                    FIRST_COL -> smartQueryTestSortingEntity.valueTwo = "valueTwo_" + generateMixedInt(i)
                    SECOND_COL -> smartQueryTestSortingEntity.valueOne = "valueOne_" + generateMixedInt(i)
                    else -> {
                        smartQueryTestSortingEntity.valueOne = "valueOne_" + generateMixedInt(i)
                        smartQueryTestSortingEntity.valueTwo = "valueTwo_" + generateMixedInt(i)
                    }
                }
            }
            oma.update(smartQueryTestSortingEntity)
        }
    }

    private fun createAscSortingQuery(): SmartQuery<SmartQueryTestSortingEntity> {
        return oma.select(SmartQueryTestSortingEntity::class.java)
                .orderAsc(SmartQueryTestSortingEntity.VALUE_ONE)
                .orderAsc(SmartQueryTestSortingEntity.VALUE_TWO)
    }

    private fun createDescSortingQuery(): SmartQuery<SmartQueryTestSortingEntity> {
        return oma.select(SmartQueryTestSortingEntity::class.java)
                .orderDesc(SmartQueryTestSortingEntity.VALUE_ONE)
                .orderDesc(SmartQueryTestSortingEntity.VALUE_TWO)
    }

    private fun isCorrectlySortedAsc(query: SmartQuery<SmartQueryTestSortingEntity>): Boolean {
        return query.count().toInt() == TWO_BLOCKS_LENGTH &&
                query.streamBlockwise().toList().first().getId() == createAscSortingQuery().queryFirst().getId() &&
                query.streamBlockwise().toList().last().getId() == createDescSortingQuery().queryFirst().getId()
    }

    private fun isCorrectlySortedDesc(query: SmartQuery<SmartQueryTestSortingEntity>): Boolean {
        return query.count().toInt() == TWO_BLOCKS_LENGTH &&
                query.streamBlockwise().toList().first().getId() == createDescSortingQuery().queryFirst().getId() &&
                query.streamBlockwise().toList().last().getId() == createAscSortingQuery().queryFirst().getId()
    }

    /**
     * Based on the index this method generates an Integer that can be used to simulate an unordered list.
     * </p>
     * If it's used in a for-loop with index e.g. 1,2,3,4 it returns 1,4,3,6,5,....
     *
     * @param index the current index of the for loop
     * @return int based on the index
     */
    private fun generateMixedInt(index: Int): Int {
        if (index % 2 == 0) {
            return index + 2
        }
        return index
    }

    companion object {
        @Part
        private lateinit var oma: OMA

        private const val TWO_BLOCKS_LENGTH = 2000
        private const val ONE_AND_HALF_BLOCK_LENGTH = 1500
        private const val ONE_BLOCK_LENGTH = 1000
        private const val FIRST_COL = 0
        private const val SECOND_COL = 1
        private const val NO_COL = -1
    }
}
