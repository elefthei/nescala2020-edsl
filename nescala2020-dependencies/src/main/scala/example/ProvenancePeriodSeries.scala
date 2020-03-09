package com.bwater.nescala

import cats.Eval
import cats.data.State
import example.Gt
import example.PeriodSeries.{CSV, Frequency, FrequencyValue, PeriodSeries}

import scala.language.implicitConversions

object ProvenancePeriodSeries {

  sealed trait Lambda[A] {
    def desc: String
    def f: A
  }
  case class Unary[A,B](desc: String, f: A => B) extends Lambda[A => B]
  case class Binary[A, B, C](desc: String, f: (A, B) => C) extends Lambda[(A, B) => C]

  sealed trait PeriodSeriesAlgebra[F <: Frequency] {
    def eval: PeriodSeries[F] = this match {
      case Roll(Extend(_, _, _), _, _) =>
        throw new AssertionError("Attempting to take a rolling window stats over an extended series, you probalby want to switch the order of operations here.")
      case Roll(Upsample(_, _, _), _, _) =>
        throw new AssertionError("Attempting to take a rolling window stats over an interpolated series, you probably want to switch the order of operations here")
      case Roll(self, namedfun, window) => self.eval.roll(namedfun.f, window)
      case Extend(self, other, ford) if ford => self.eval.extendFord(other.eval)
      case Extend(self, other, _) => self.eval.extendBack(other.eval)
      case Upsample(self, newfreq, gt) => self.eval.upSample(newfreq = newfreq)(gt)
      case Downsample(self, newfreq, gt) => self.eval.downSample(newfreq = newfreq)(gt)
      case Intersect(self, other, namedop) => self.eval.intersection(other.eval, namedop.f)
      case Pure(self) => self
    }
  }
  case class Roll[F <: Frequency](self: PeriodSeriesAlgebra[F], f: Lambda[PeriodSeries[F] => Double], window: Int) extends PeriodSeriesAlgebra[F]
  case class Extend[F <: Frequency](self: PeriodSeriesAlgebra[F], other: PeriodSeriesAlgebra[F], ford :Boolean) extends PeriodSeriesAlgebra[F]
  case class Upsample[F <: Frequency, FF <: Frequency](self: PeriodSeriesAlgebra[F], freq: FrequencyValue[FF], gt: Gt[FF, F]) extends PeriodSeriesAlgebra[FF]
  case class Downsample[F <: Frequency, FF <: Frequency](self: PeriodSeriesAlgebra[F], freq: FrequencyValue[FF], gt: Gt[F, FF]) extends PeriodSeriesAlgebra[FF]
  case class Intersect[F <: Frequency](self: PeriodSeriesAlgebra[F], other: PeriodSeriesAlgebra[F], op: Lambda[(Double, Double) => Double]) extends PeriodSeriesAlgebra[F]
  case class Pure[F <: Frequency](p: PeriodSeries[F]) extends PeriodSeriesAlgebra[F]

  // Some Syntactic sugar for our eDSL
  implicit class PeridSeriesPure[F <: Frequency](ps: PeriodSeries[F]){
    def reify: PeriodSeriesAlgebra[F] = Pure(ps)
  }
  implicit class PeriodSeriesAlgebraSyntax[F <: Frequency](self: PeriodSeriesAlgebra[F]) {
    def rollMean(window: Int = 2): PeriodSeriesAlgebra[F] = Roll(self, Unary("Mean", _.mean), window)
    def rollStdDev(window: Int = 2): PeriodSeriesAlgebra[F] = Roll(self, Unary("Std Dev", _.stdDev), window)
    def rollZscore(window: Int = 2): PeriodSeriesAlgebra[F] = Roll(self, Unary("Z", p => p.mean / p.stdDev), window)
    def maChange(front: Int = 1, back: Int = 12): PeriodSeriesAlgebra[F] = Roll(self, Unary("Smoothed Change", ps => ps.copy(data = ps.data.take(front)).mean - ps.mean), back)
    def extendBack(other: PeriodSeriesAlgebra[F]): PeriodSeriesAlgebra[F] = Extend(self, other, ford = false)
    def extendFord(other: PeriodSeriesAlgebra[F]): PeriodSeriesAlgebra[F] = Extend(self, other, ford = true)
    def upSample[FF <: Frequency](newfreq: FrequencyValue[FF])(implicit ev: Gt[FF, F]): PeriodSeriesAlgebra[FF] = Upsample[F, FF](self, newfreq, ev)
    def downSample[FF <: Frequency](newfreq: FrequencyValue[FF])(implicit ev: Gt[F, FF]): PeriodSeriesAlgebra[FF] = Downsample[F, FF](self, newfreq, ev)
    def +(other: PeriodSeriesAlgebra[F]): PeriodSeriesAlgebra[F] = Intersect(self, other, Binary("Add", _ + _))
    def -(other: PeriodSeriesAlgebra[F]): PeriodSeriesAlgebra[F] = Intersect(self, other, Binary("Sub", _ - _))
    def *(other: PeriodSeriesAlgebra[F]): PeriodSeriesAlgebra[F] = Intersect(self, other, Binary("Mult", _ * _))
    def /(other: PeriodSeriesAlgebra[F]): PeriodSeriesAlgebra[F] = Intersect(self, other, Binary("Div", _ / _))
  }

  // Provenance is just interleaved state
  type Provenance[F <: Frequency] = State[Seq[String], PeriodSeries[F]]

  implicit class PeriodSeriesEvalWithState[F <: Frequency](self: PeriodSeriesAlgebra[F]) {
    def evalWithProvenance: Provenance[F] = self match {
      case Roll(Extend(_, _, _), _, _) =>
        throw new AssertionError("Attempting to take a rolling window stats over an extended series, you probalby want to switch the order of operations here.")
      case Roll(Upsample(_, _, _), _, _) =>
        throw new AssertionError("Attempting to take a rolling window stats over an interpolated series, you probably want to switch the order of operations here")
      case Roll(self, Unary(desc, f), window) =>
        self.evalWithProvenance.flatMap { series =>
          State(st => (st :+ s"Roll with $desc and window = $window", series.roll(f, window)))
        }
      case Extend(self, other, ford) if ford =>
        self.evalWithProvenance.flatMap { series =>
          val extension = other.eval
          State(st => (st :+ s"Extend forward with $extension", series.extendFord(extension)))
        }
      case Extend(self, other, _) =>
        self.evalWithProvenance.flatMap { series =>
          val extension = other.eval
          State(st => (st :+ s"Extend backwards with $extension", series.extendBack(extension)))
        }
      case Upsample(self, newfreq, gt) =>
        self.evalWithProvenance.flatMap { series =>
          val upsampled = series.upSample(newfreq)(gt)
          State(st => (st :+ s"Up sample to $newfreq", upsampled))
        }
      case Downsample(self, newfreq, gt) =>
        self.evalWithProvenance.flatMap { series =>
          val downsampled = series.downSample(newfreq)(gt)
          State(st => (st :+ s"Down sample to $newfreq", downsampled))
        }
      case Intersect(self, other, Binary(desc, f)) =>
        self.evalWithProvenance.flatMap { series =>
          other.evalWithProvenance.flatMap { other =>
            State(st => (st :+ s"Combine with $desc and $other", series.intersection(other, f)))
          }
        }
      case Pure(self) => State.pure(self)
    }

    def provenance = evalWithProvenance.run(Seq()).value._1
    def eval = evalWithProvenance.run(Seq()).value._2
  }
}

object Prov extends App {
  import ProvenancePeriodSeries._
  val ps = CSV.loadMonthlyData("/Users/lef/git/nescala2020-eDSLs/interest-rates/interest-rates-3m.txt")
  ps.reify
      .maChange(1,18)
      .rollStdDev(2)
      .upSample(Frequency.Daily)
      .provenance
      .foreach(println)
}
