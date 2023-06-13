package settings

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/**
 * BonusSettings contains all the settings for the bonus calculation.
 */
object BonusSettings {
  val TOTAL_SALES_BONUS_PERCENTAGE: Double = 0.1 // The percentage of the total sales that is used for the bonus calculation (for all employees)
  private val ADDITIONAL_BONUS_POOL: Double = 1000.0 // The pool of money that is used for the additional bonus
  private val ADDITIONAL_BONUS_PERCENTAGES: List[Double] = List(0.55, 0.25, 0.1, 0.07, 0.03) // The percentages of the additional bonus pool for each place in the ranking
  private val ADDITIONAL_BONUS_VALUES: List[Double] = ADDITIONAL_BONUS_PERCENTAGES.map(x => x * ADDITIONAL_BONUS_POOL)
  val ADDITIONAL_BONUS_POOL_SIZE: Int = ADDITIONAL_BONUS_VALUES.size

  /*
  * Returns the additional bonus value for a given index
  * Can be treated as a function which for given index returns the bonus value
  * where first index is the first place in the ranking, second index is the second place in the ranking etc.
  */
  private def getAdditionalBonus(index: Int): Double = {
    ADDITIONAL_BONUS_VALUES(index)
  }

  /*
  * UDF (User-Defined Function) to retrieve array elements based on row numbers
  */
  def getAdditionalBonusUDF: UserDefinedFunction = udf((index: Int) => getAdditionalBonus(index - 1))
}
