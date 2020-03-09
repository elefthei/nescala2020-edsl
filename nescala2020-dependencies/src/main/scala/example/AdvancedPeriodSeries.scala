package com.bwater.nescala

import java.io.File

import cats.data.State
import com.bwater.nescala.AdvancedPeriodSeries.PeriodSeriesAlgebra
import example.Gt
import example.PeriodSeries.{Frequency, FrequencyValue, PeriodSeries}
import org.joda.time.{DateTime, Duration}
import org.joda.time.format.DateTimeFormatter
import sun.security.ec.point.ProjectivePoint.Mutable
import example.PeriodSeries.PeriodSeries._

import scala.language.implicitConversions
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.math.Ordered._

object AdvancedPeriodSeries {
  sealed trait PeriodSeriesAlgebra[F <: Frequency] {
    def eval: PeriodSeries[F] = this match {
      case Roll(Extend(_, _, _), _, _, _) =>
        throw new AssertionError("Attempting to take a rolling window stats over an extended series, you probalby want to switch the order of operations here.")
      case Roll(Upsample(_, _, _), _, _, _) =>
        throw new AssertionError("Attempting to take a rolling window stats over an interpolated series, you probably want to switch the order of operations here")
      case Roll(self, f, window, _) => self.eval.roll(f, window)
      case Extend(self, other, ford) if ford => self.eval.extendFord(other.eval)
      case Extend(self, other, _) => self.eval.extendBack(other.eval)
      case Upsample(self, newfreq, gt) => self.eval.upSample(newfreq = newfreq)(gt)
      case Downsample(self, newfreq, gt) => self.eval.downSample(newfreq = newfreq)(gt)
      case Intersect(self, other, op, _) => self.eval.intersection(other.eval, op)
      case Pure(self) => self
    }
  }
  case class Roll[F <: Frequency](self: PeriodSeriesAlgebra[F], f: PeriodSeries[F] => Double, window: Int, desc: String) extends PeriodSeriesAlgebra[F]
  case class Extend[F <: Frequency](self: PeriodSeriesAlgebra[F], other: PeriodSeriesAlgebra[F], ford :Boolean) extends PeriodSeriesAlgebra[F]
  case class Upsample[F <: Frequency, FF <: Frequency](self: PeriodSeriesAlgebra[F], freq: FrequencyValue[FF], gt: Gt[FF, F]) extends PeriodSeriesAlgebra[FF]
  case class Downsample[F <: Frequency, FF <: Frequency](self: PeriodSeriesAlgebra[F], freq: FrequencyValue[FF], gt: Gt[F, FF]) extends PeriodSeriesAlgebra[FF]
  case class Intersect[F <: Frequency](self: PeriodSeriesAlgebra[F], other: PeriodSeriesAlgebra[F], op: (Double, Double) => Double, desc: String) extends PeriodSeriesAlgebra[F]
  case class Pure[F <: Frequency](p: PeriodSeries[F]) extends PeriodSeriesAlgebra[F]

  // Some Syntactic sugar for our eDSL
  implicit class PeridSeriesPure[F <: Frequency](ps: PeriodSeries[F]){
    def reify: PeriodSeriesAlgebra[F] = Pure(ps)
  }
  implicit class PeriodSeriesAlgebraSyntax[F <: Frequency](self: PeriodSeriesAlgebra[F]) {
    def rollMean(window: Int = 2): PeriodSeriesAlgebra[F] = Roll(self, _.mean, window, "Mean")
    def rollStdDev(window: Int = 2): PeriodSeriesAlgebra[F] = Roll(self, _.stdDev, window, "StdDev")
    def rollZscore(window: Int = 2): PeriodSeriesAlgebra[F] = Roll(self, p => p.mean / p.stdDev, window, "Z")
    def maChange(front: Int = 1, back: Int = 12): PeriodSeriesAlgebra[F] = Roll(self, ps => ps.copy(data = ps.data.take(front)).mean - ps.mean, back, "Smoothed Change")
    def extendBack(other: PeriodSeriesAlgebra[F]): PeriodSeriesAlgebra[F] = Extend(self, other, ford = false)
    def extendFord(other: PeriodSeriesAlgebra[F]): PeriodSeriesAlgebra[F] = Extend(self, other, ford = true)
    def upSample[FF <: Frequency](newfreq: FrequencyValue[FF])(implicit ev: Gt[FF, F]): PeriodSeriesAlgebra[FF] = Upsample[F, FF](self, newfreq, ev)
    def downSample[FF <: Frequency](newfreq: FrequencyValue[FF])(implicit ev: Gt[F, FF]): PeriodSeriesAlgebra[FF] = Downsample[F, FF](self, newfreq, ev)
    def +(other: PeriodSeriesAlgebra[F]): PeriodSeriesAlgebra[F] = Intersect(self, other, _ + _, "add")
    def -(other: PeriodSeriesAlgebra[F]): PeriodSeriesAlgebra[F] = Intersect(self, other, _ - _, "subtract")
    def *(other: PeriodSeriesAlgebra[F]): PeriodSeriesAlgebra[F] = Intersect(self, other, _ * _, "multiply")
    def /(other: PeriodSeriesAlgebra[F]): PeriodSeriesAlgebra[F] = Intersect(self, other, _ / _, "divide")
  }
}
