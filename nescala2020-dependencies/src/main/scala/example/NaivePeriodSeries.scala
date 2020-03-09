package com.bwater.nescala

import java.io.File

import com.bwater.nescala.NaivePeriodSeries.PeriodSeries
import org.joda.time.Duration
import org.joda.time.format.DateTimeFormatter
import sun.security.ec.point.ProjectivePoint.Mutable

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.math.Ordered._

object NaivePeriodSeries {
  import org.joda.time.DateTime
  import org.joda.time.format.DateTimeFormat

  // Create some enumerations
  object Frequency extends Enumeration {
    type Inner = Value
    val Daily, Monthly, Quarterly, Yearly = Value
    def fromString(s: String): Frequency =
      s match {
        case "Yearly" => Yearly
        case "Monthly" => Monthly
        case "Quarterly" => Quarterly
        case "Daily" => Daily
      }
  }
  type Frequency = Frequency.Inner
  implicit class FreqToDays(f: Frequency) {
    def days(): Int =
      f match {
        case Frequency.Monthly => 30
        case Frequency.Quarterly => 30 * 4
        case Frequency.Yearly => 365
        case _ => 1
      }
  }
  implicit val FrequencyOrdered = new Ordering[Frequency]{
    def compare(a: Frequency, b: Frequency): Int = a.days.compare(b.days)
  }
  implicit val DateTimeOrdered = new Ordering[DateTime] {
    override def compare(x: DateTime, y: DateTime): Int = x.getMillis.compare(y.getMillis)
  }

  // PeriodSeries naive implementation starts here
  case class PeriodSeries(data: Array[Double], frequency: Frequency, base: DateTime)
  object CSV {
    def load(csv: String): PeriodSeries = {
      val file = Source.fromFile(csv).getLines().toSeq
      val freq = Frequency.fromString(file.head)
      val base = DateTime.parse(file.tail.head, DateTimeFormat.forPattern("yyyy"))
      val data = file.tail.tail.map(_.toDouble).toArray
      PeriodSeries(data, freq, base)
    }
  }

  implicit class PeriodSeriesMath(ps: PeriodSeries) {
    def length: Int = ps.data.length
    def first: Double = ps.data.head
    def last: Double = ps.data.last
    def enddate: DateTime =
      ps.base.plusDays(ps.frequency.days() * ps.data.length)
    def mean: Double = ps.data.sum / ps.data.length
    def stdDev: Double = {
      val avg = ps.mean
      math.sqrt(ps.data.map(a => math.pow(a - avg, 2)).sum) / ps.data.length
    }

    def toTicks: Seq[(DateTime, Double)] = ps.data.toIndexedSeq.zipWithIndex.map { case (d, i) => (ps.base.plusDays(ps.frequency.days * i), d) }
    def roll(f: PeriodSeries => Double, window: Int = 2): PeriodSeries = {
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

    def maChange(front: Int = 1, back: Int = 12): PeriodSeries = roll(ps => ps.copy(data = ps.data.take(front)).mean - ps.mean, back)
    def rollMean(window: Int = 2): PeriodSeries = roll(_.mean)
    def rollStdDev(window: Int = 2): PeriodSeries = roll(_.stdDev)
    def rollZscore(window: Int = 2): PeriodSeries = roll(p => p.mean / p.stdDev)
    def +(other: PeriodSeries): PeriodSeries = {
      val ord = Ordering[DateTime]
      require(ps.frequency == other.frequency, "Cannot add PeriodSeries of different frequencies")
      require(ord.max(ps.base, other.base) <= ord.min(ps.enddate, other.enddate), "Cannot add PeriodSeries that do not overlap")
      ps.copy(data = ps.data.zip(other.data).map(p => p._1 + p._2), base = ord.max(ps.base, other.base))
    }
  }

  implicit class PeriodSeriesExtensions(ps: PeriodSeries) {
    import scala.math.Ordered._

    def truncEnd(cutoff: DateTime): PeriodSeries = {
      require(ps.base <= cutoff && ps.enddate >= cutoff , s"Cannot truncate at either past or future date $cutoff")
      ps.copy(data = ps.toTicks.filter(_._1 <= cutoff).map(_._2).toArray)
    }
    def truncStart(cutoff: DateTime): PeriodSeries = {
      require(ps.base <= cutoff && ps.enddate >= cutoff , s"Cannot truncate at either past or future date $cutoff")
      ps.copy(data = ps.toTicks.filter(_._1 >= cutoff).map(_._2).toArray, base = cutoff)
    }

    def downSample(newfreq: Frequency) : PeriodSeries = {
      require(newfreq >= ps.frequency, "Cannot down-sample with higher frequency")
      val step = (newfreq.days / ps.frequency.days).toInt
      def groupByNum[A](s: Iterable[A], n: Int): Iterable[Iterable[A]] =
        if(n == 0) {
          Seq()
        } else if(s.size <= n) {
          Seq(s)
        } else {
          val (left, right) = s.splitAt(n)
          Seq(left) ++ groupByNum(right.tail, n)
        }
      ps.copy(data = groupByNum(ps.data, step).map(data => data.sum / data.size).toArray, frequency = newfreq)
    }

    private def interpolate(first: Double, second: Double, steps: Int): Array[Double] =
      (0 to steps).map(_ * (second - first) / steps).map(_ + first).toArray

    def upSample(newfreq: Frequency): PeriodSeries = {
      require(newfreq <= ps.frequency, "Cannot up-sample with lower frequency")
      val steps = ps.frequency.days / newfreq.days
      val seq = ps.data.toIndexedSeq.zipWithIndex.flatMap {
        case (_, i) if i < ps.data.length - 1 => interpolate(ps.data(i), ps.data(i + 1), steps)
        case (d, _) => Array(d)
      }.toArray
      ps.copy(data = seq, frequency = newfreq)
    }

    def extendBack(other: PeriodSeries): PeriodSeries = {
      require(other.base <= ps.base, s"Cannot extend back with less back-history")
      require(ps.frequency == other.frequency, s"Cannot extend with different frequency ${other.frequency}")
      val result = ArrayBuffer[Double]()
      if(other.enddate <= ps.base) {
        // other <> [GAP] <> ps
        val steps = (new Duration(other.enddate, ps.base).getStandardDays / ps.frequency.days()).toInt
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

    def extendForward(other: PeriodSeries): PeriodSeries = {
      require(other.enddate >= ps.enddate, s"Cannot extend forward with less forward-history")
      require(ps.frequency == other.frequency, s"Cannot extend with different frequency ${other.frequency}")
      val result = ArrayBuffer[Double]()
      if(other.base >= ps.enddate) {
        // ps <> [GAP] <> other
        val steps = (new Duration(ps.enddate, other.base).getStandardDays / ps.frequency.days()).toInt
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

object Foo extends App {
  import NaivePeriodSeries._

  val ps = CSV.load("~/git/nescala2020-eDSLs/interest-rates/interest-rates-3m.txt")
  ps.roll(ps => { println(ps.data.toSeq.toString()); ps.last }, window = 2)

  println(ps)
  println(ps.rollStdDev(window = 4))
}
