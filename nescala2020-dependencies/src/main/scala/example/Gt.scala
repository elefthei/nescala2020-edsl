package example

import example.PeriodSeries.Frequency.{Daily, Monthly, Quarterly, Yearly}
import example.PeriodSeries.{Frequency, FrequencyValue}

trait Gt[Small <: Frequency, Large <: Frequency] {
  def smaller: FrequencyValue[Small]
  def large: FrequencyValue[Large]
}
object Gt {
  implicit val dailyLtMonthly: Gt[Daily, Monthly] = new Gt[Daily, Monthly] {
    override def smaller: FrequencyValue[Daily] = Daily
    override def large: FrequencyValue[Monthly] = Monthly
  }
  implicit val dailyLtQuarterly: Gt[Daily, Quarterly] = new Gt[Daily, Quarterly] {
    override def smaller: FrequencyValue[Daily] = Daily
    override def large: FrequencyValue[Quarterly] = Quarterly
  }
  implicit val dailyLtYearly: Gt[Daily, Yearly] = new Gt[Daily, Yearly] {
    override def smaller: FrequencyValue[Daily] = Daily
    override def large: FrequencyValue[Yearly] = Yearly
  }
  implicit val monthlyLtYearly: Gt[Monthly, Yearly] = new Gt[Monthly, Yearly] {
    override def smaller: FrequencyValue[Monthly] = Monthly
    override def large: FrequencyValue[Yearly] = Yearly
  }
  implicit val monthlyLtQuarterly: Gt[Monthly, Quarterly] = new Gt[Monthly, Quarterly] {
    override def smaller: FrequencyValue[Monthly] = Monthly
    override def large: FrequencyValue[Quarterly] = Quarterly
  }
  implicit val quarterlyLtYearly: Gt[Quarterly, Yearly] = new Gt[Quarterly, Yearly] {
    override def smaller: FrequencyValue[Quarterly] = Quarterly
    override def large: FrequencyValue[Yearly] = Yearly
  }
}

