package example

import java.io.File

import example.PeriodSeries.CSV
import example.PeriodSeries.Frequency.{Daily, Monthly, Quarterly, Yearly}
import org.joda.time.Duration
import org.joda.time.format.DateTimeFormatter
import sun.security.ec.point.ProjectivePoint.Mutable
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.math.Ordered._

object PeriodSeries {
  sealed trait Frequency
  sealed abstract class FrequencyValue[F <: Frequency] {
    def days: Int
  }

  object Frequency {
    sealed trait Daily extends Frequency
    sealed trait Monthly extends Frequency
    sealed trait Quarterly extends Frequency
    sealed trait Yearly extends Frequency

    case object Daily extends FrequencyValue[Daily] {
      override def days: Int = 1
    }
    case object Monthly extends FrequencyValue[Monthly] {
      override def days: Int = 30
    }
    case object Quarterly extends FrequencyValue[Quarterly] {
      override def days: Int = 30 * 4
    }
    case object Yearly extends FrequencyValue[Yearly] {
      override def days: Int = 365
    }
  }

  implicit val DateTimeOrdered = new Ordering[DateTime] {
    override def compare(x: DateTime, y: DateTime): Int = x.getMillis.compare(y.getMillis)
  }

  // PeriodSeries naive implementation starts here
  case class PeriodSeries[F <: Frequency](data: Array[Double], frequency: FrequencyValue[F], base: DateTime)
  object CSV {
    def loadMonthlyData(csv: String): PeriodSeries[Frequency.Monthly] = {
      val file = Source.fromFile(csv).getLines().toSeq
      require(file.head == "Monthly", "Cannot load Data which not Monthly with this method")
      val base = DateTime.parse(file.tail.head, DateTimeFormat.forPattern("yyyy"))
      val data = file.tail.tail.map(_.toDouble).toArray
      PeriodSeries(data, Frequency.Monthly, base)
    }
    def loadQuarterlyData(csv: String): PeriodSeries[Frequency.Quarterly] = {
      val file = Source.fromFile(csv).getLines().toSeq
      require(file.head == "Quarterly", "Cannot load Data which not Quarterly with this method")
      val base = DateTime.parse(file.tail.head, DateTimeFormat.forPattern("yyyy"))
      val data = file.tail.tail.map(_.toDouble).toArray
      PeriodSeries(data, Frequency.Quarterly, base)
    }
    def loadYearlyData(csv: String): PeriodSeries[Frequency.Yearly] = {
      val file = Source.fromFile(csv).getLines().toSeq
      require(file.head == "Yearly", "Cannot load Data which not Yearly with this method")
      val base = DateTime.parse(file.tail.head, DateTimeFormat.forPattern("yyyy"))
      val data = file.tail.tail.map(_.toDouble).toArray
      PeriodSeries(data, Frequency.Yearly, base)
    }
  }

  implicit class PeriodSeriesMath[F <: Frequency](ps: PeriodSeries[F]) {
    def length: Int = ps.data.length
    def first: Double = ps.data.head
    def enddate: DateTime =
      ps.base.plusDays(ps.frequency.days * ps.data.length)
    def last: Double = ps.data.last
    def mean: Double = ps.data.sum / ps.data.length
    def stdDev: Double = {
      val avg = ps.mean
      math.sqrt(ps.data.map(a => math.pow(a - avg, 2)).sum) / ps.data.length
    }

    def toTicks: Seq[(DateTime, Double)] = ps.data.toIndexedSeq.zipWithIndex.map { case (d, i) => (ps.base.plusDays(ps.frequency.days * i), d) }
    def roll(f: PeriodSeries[F] => Double, window: Int = 2): PeriodSeries[F] = {
      require(ps.data.length > window, "Window too big for small dataset")
      var i = 0
      val result: ArrayBuffer[Double] = ArrayBuffer()
      while (i + window < ps.data.length) {
        val newps = ps.copy(data = ps.data.slice(i, i + window))
        result.append(f(newps))
        i += 1
      }
      PeriodSeries(result.toArray, ps.frequency, ps.base)
    }

    // Combine by intersection
    def intersection(other: PeriodSeries[F], op: (Double, Double) => Double): PeriodSeries[F] = {
      val ord = Ordering[DateTime]
      require(ord.max(ps.base, other.base) <= ord.min(ps.enddate, other.enddate), "Cannot zip PeriodSeries that do not overlap")
      ps.copy(data = ps.data.zip(other.data).map((p: (Double, Double)) => op(p._1, p._2)), base = ord.max(ps.base, other.base))
    }
    def maChange(front: Int = 1, back: Int = 12): PeriodSeries[F] = roll(ps => ps.copy(data = ps.data.take(front)).mean - ps.mean, back)
    def rollMean(window: Int = 2): PeriodSeries[F] = roll(_.mean)
    def rollStdDev(window: Int = 2): PeriodSeries[F] = roll(_.stdDev)
    def rollZscore(window: Int = 2): PeriodSeries[F] = roll(p => p.mean / p.stdDev)
    def +(other: PeriodSeries[F]): PeriodSeries[F] = intersection(other, _ + _)
    def -(other: PeriodSeries[F]): PeriodSeries[F] = intersection(other, _ - _)
    def *(other: PeriodSeries[F]): PeriodSeries[F] = intersection(other, _ * _)
    def /(other: PeriodSeries[F]): PeriodSeries[F] = intersection(other, _ / _)
  }

  // Extensions
  implicit class PeriodSeriesExtensions[F <: Frequency](ps: PeriodSeries[F]) {
    import scala.math.Ordered._

    def truncEnd(cutoff: DateTime): PeriodSeries[F] = {
      require(ps.base <= cutoff && ps.enddate >= cutoff , s"Cannot truncate at either past or future date $cutoff")
      ps.copy(data = ps.toTicks.filter(_._1 <= cutoff).map(_._2).toArray)
    }
    def truncStart(cutoff: DateTime): PeriodSeries[F] = {
      require(ps.base <= cutoff && ps.enddate >= cutoff , s"Cannot truncate at either past or future date $cutoff")
      ps.copy(data = ps.toTicks.filter(_._1 >= cutoff).map(_._2).toArray, base = cutoff)
    }

    def downSample[FF <: Frequency](newfreq: FrequencyValue[FF])(implicit lt: Gt[F, FF]) : PeriodSeries[FF] = {
      val step: Int = lt.large.days / lt.smaller.days
      def groupByNum[A](s: Iterable[A], n: Int): Iterable[Iterable[A]] =
        if(n == 0) {
          Seq()
        } else if(s.size <= n) {
          Seq(s)
        } else {
          val (left, right) = s.splitAt(n)
          Seq(left) ++ groupByNum(right.tail, n)
        }
      ps.copy(data = groupByNum(ps.data, step).map(data => data.sum / data.size).toArray, frequency = lt.large)
    }

    private def interpolate(first: Double, second: Double, steps: Int): Array[Double] =
      (0 to steps).map(_ * (second - first) / steps).map(_ + first).toArray

    def upSample[FF <: Frequency](newfreq: FrequencyValue[FF])(implicit lt: Gt[FF, F]): PeriodSeries[FF] = {
      val steps = lt.large.days / lt.smaller.days
      val seq = ps.data.toIndexedSeq.zipWithIndex.flatMap {
        case (_, i) if i < ps.data.length - 1 => interpolate(ps.data(i), ps.data(i + 1), steps)
        case (d, _) => Array(d)
      }.toArray
      ps.copy(data = seq, frequency = lt.smaller)
    }

    def extendBack(other: PeriodSeries[F]): PeriodSeries[F] = {
      require(other.base <= ps.base, s"Cannot extend back with less back-history")
      val result = ArrayBuffer[Double]()
      if(other.enddate <= ps.base) {
        // other <> [GAP] <> ps
        val steps = (new Duration(other.enddate, ps.base).getStandardDays / ps.frequency.days).toInt
        result.appendAll(other.data)
        result.appendAll(interpolate(other.last, ps.first, steps))
        result.appendAll(ps.data)
        PeriodSeries(data = result.toArray, frequency = ps.frequency, base = other.base)
      } else {
        // other <> ps
        result.appendAll(other.truncEnd(ps.base).data)
        result.appendAll(ps.data)
        PeriodSeries(data = result.toArray, frequency = ps.frequency, base = other.base)
      }
    }

    def extendFord(other: PeriodSeries[F]): PeriodSeries[F] = {
      require(other.enddate >= ps.enddate, s"Cannot extend forward with less forward-history")
      val result = ArrayBuffer[Double]()
      if(other.base >= ps.enddate) {
        // ps <> [GAP] <> other
        val steps = (new Duration(ps.enddate, other.base).getStandardDays / ps.frequency.days).toInt
        result.appendAll(ps.data)
        result.appendAll(interpolate(ps.last, other.first, steps))
        result.appendAll(other.data)
        PeriodSeries(data = result.toArray, frequency = ps.frequency, base = ps.base)
      } else {
        // ps <> other
        result.appendAll(ps.data)
        result.appendAll(other.truncStart(ps.enddate).data)
        PeriodSeries(data = result.toArray, frequency = ps.frequency, base = ps.base)
      }
    }
  }
}

object Bar extends App {

  val ps = CSV.loadMonthlyData("/Users/lef/git/nescala2020-eDSLs/interest-rates/interest-rates-3m.txt")
  println(ps)
  println(ps.rollStdDev(window = 4))
}
