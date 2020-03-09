package example

import scala.collection.immutable.HashMap

object LMapOps {

  // Let's construct expressions for a lazy HashMap
  sealed trait LMap[F, U] extends Product with Serializable {
    // Implement these
    def run: HashMap[F, U]

    def map[H, I](f: (F, U) => (H, I)): LMap[H, I]

    def flatMap[H, I](f: (F, U) => LMap[H, I]): LMap[H, I]

    def foldLeft[G, B](z: LMap[G, B], f: LMap[G, B] => (F, U) => LMap[G, B]): LMap[G, B]

    def join[C](r: LMap[F, C]): LMap[F, (U, C)]

    // For provenance
    def trace(): String

    // Initializers
    def empty[H, I](): LMap[H, I] = HashMap[H, I]().reify

    // Get these for free!
    def collect[H, I](f: (F, U) PartialFunction (H, I)): LMap[H, I] =
      flatMap((k: F, v: U) => f.lift(k, v) match {
        case None => empty[H, I]()
        case Some(out) => LMap(out._1 -> out._2)
      })

    def filter(f: (F, U) => Boolean): LMap[F, U] =
      collect { case (k, v) if f(k, v) => (k, v) }

    // Add provenance interleaved with ops
    def provenance(op: String): LMap[F, U] =
      Provenance(op, this)

    def groupBy[G](f: F => G): LMap[G, LMap[F, U]] =
      map { case (k, _) => (f(k), this) } // HashMap will drop duplicates

    // Z can be anything, including a primitive type. Evaluate
    def foldLeft[Z](z: Z, f: (Z, (F, U)) => Z): Z =
      run.foldLeft(z)(f)

    def foldRight[Z](z: Z, f: ((F, U), Z) => Z): Z =
      run.foldRight(z)(f)

    def flattenTo[G]: LMap[G, U] = ??? // implement with groupBy, don't have CanFlattenTo
  }

  // Companion object
  object LMap {
    def apply[H, I](args: (H, I)*): LMap[H, I] =
      HashMap[H, I](args: _*).reify
  }

  final case class Provenance[F, U](op: String, fa: LMap[F, U]) extends LMap[F, U] {
    override def run: HashMap[F, U] = fa.run

    override def map[H, I](f: (F, U) => (H, I)): LMap[H, I] =
      Map(this, f)

    override def flatMap[H, I](f: (F, U) => LMap[H, I]): LMap[H, I] =
      FlatMap(this, f)

    override def foldLeft[G, B](z: LMap[G, B], f: LMap[G, B] => (F, U) => LMap[G, B]): LMap[G, B] =
      Provenance(op, fa.foldLeft(z, f))

    override def join[C](r: LMap[F, C]): LMap[F, (U, C)] =
      Provenance(op, fa.join(r))

    override def trace(): String = fa.trace() ++ op ++ "\n" ++ fa.run.toString() ++ "\n\n"
  }

  final case class Map[F, G, U, B](fa: LMap[F, U], f: (F, U) => (G, B))
    extends LMap[G, B] {
    override def run: HashMap[G, B] = fa.run.map { case (k, v) => f(k, v) }

    // Optimization!
    override def map[H, I](g: (G, B) => (H, I)): LMap[H, I] =
      Map(fa, (a: F, b: U) => {
        val (x, y) = f(a, b) // Compose f . g with tuples
        g(x, y)
      })

    override def join[V](r: LMap[G, V]): LMap[G, (B, V)] =
      Join(this, r)

    override def flatMap[H, I](g: (G, B) => LMap[H, I]): LMap[H, I] =
      FlatMap(this, g)

    override def foldLeft[H, I](z: LMap[H, I], f: LMap[H, I] => (G, B) => LMap[H, I]): LMap[H, I] =
      Fold(this, z, f)

    // Only print provenance
    override def trace(): String = fa.trace
  }

  final case class FlatMap[F, U, G, B](fa: LMap[F, U], f: (F, U) => LMap[G, B])
    extends LMap[G, B] {
    override def run: HashMap[G, B] = fa.run.flatMap { case (k, v) => f(k, v).run.toIterable }

    override def map[H, I](g: (G, B) => (H, I)): LMap[H, I] =
      Map(this, g)

    override def join[V](r: LMap[G, V]): LMap[G, (B, V)] =
      Join(this, r)

    // Optimization
    override def flatMap[H, I](g: (G, B) => LMap[H, I]): LMap[H, I] =
      FlatMap(fa, (k: F, v: U) => f(k, v).flatMap(g))

    override def foldLeft[H, I](z: LMap[H, I], f: LMap[H, I] => (G, B) => LMap[H, I]): LMap[H, I] =
      Fold(this, z, f)

    // Only print provenance
    override def trace(): String = fa.trace
  }

  // Fold Expressions
  final case class Fold[F, U, G, B](fa: LMap[F, U], z: LMap[G, B], f: LMap[G, B] => (F, U) => LMap[G, B])
    extends LMap[G, B] {
    override def run: HashMap[G, B] = fa.run.foldLeft(z) { case (h, (k, v)) => f(h)(k, v) }.run

    override def map[H, I](g: (G, B) => (H, I)): LMap[H, I] =
      Map(this, g)

    override def join[V](r: LMap[G, V]): LMap[G, (B, V)] =
      Join(this, r)

    override def flatMap[H, I](g: (G, B) => LMap[H, I]): LMap[H, I] =
      FlatMap(this, g)

    // Optimize me!
    override def foldLeft[H, I](s: LMap[H, I], g: LMap[H, I] => (G, B) => LMap[H, I]): LMap[H, I] =
      Fold(this, s, g)

    // Only print provenance
    override def trace(): String = fa.trace
  }

  // Join expressions
  final case class Join[F, B, C](l: LMap[F, B], r: LMap[F, C])
    extends LMap[F, (B, C)] {
    override def run: HashMap[F, (B, C)] = {
      // Real join implementation, pretty inefficient but succinct
      val left: HashMap[F, B] = l.run
      val right: HashMap[F, C] = r.run

      // Annoying way to initialize a HashMap
      new HashMap[F, (B, C)]() ++ left.keySet.union(right.keySet)
        .map(k =>
          (k, (left.getOrElse(k, throw new IllegalArgumentException(s"Left field $k not found")),
            right.getOrElse(k, throw new IllegalArgumentException(s"Right field $k not found")))))
        .toMap
    }

    override def map[H, I](g: (F, (B, C)) => (H, I)): LMap[H, I] =
      Map(this, g)

    // If I had my trusty MultiJoin3 here it would be piece of cake
    override def join[D](r: LMap[F, D]): LMap[F, ((B, C), D)] =
      Join(this, r)

    override def flatMap[H, I](g: (F, (B, C)) => LMap[H, I]): LMap[H, I] =
      FlatMap(this, g)

    override def foldLeft[H, I](z: LMap[H, I], f: LMap[H, I] => (F, (B, C)) => LMap[H, I]): LMap[H, I] =
      Fold(this, z, f)

    // Print provenance
    override def trace(): String = "JOIN {\n" ++ l.trace() ++ ",\n" ++ r.trace() ++ "}\n"
  }

  // Algebra entry point
  final case class Single[F, B](in: HashMap[F, B]) extends LMap[F, B] {
    override def run: HashMap[F, B] = in

    override def map[H, I](g: (F, B) => (H, I)): LMap[H, I] =
      Map(this, g)

    // If I had my trusty MultiJoin3 here it would be piece of cake
    override def join[C](r: LMap[F, C]): LMap[F, (B, C)] =
      Join(this, r)

    override def flatMap[H, I](g: (F, B) => LMap[H, I]): LMap[H, I] =
      FlatMap(this, g)

    override def foldLeft[H, I](z: LMap[H, I], f: LMap[H, I] => (F, B) => LMap[H, I]): LMap[H, I] =
      Fold(this, z, f)

    // Only print provenance
    override def trace(): String = ""
  }

  // Lift HashMap to Lazy Map
  implicit class LazyOps[F, U](h: HashMap[F, U]) {
    def reify: LMap[F, U] = Single(h)
  }
}

object ADHTApp extends App {
  import LMapOps._
  // ===============================================
  // Main body begins here
  // ===============================================
  // I got some stocks here
  val stocks: HashMap[String, Double] = HashMap("AAPL" -> 180.26, "ABC" -> 200.00)

  // Apply some lazy functions
  val lazystocks =
    stocks
      .reify
      .provenance("Initial stock offering")
      .map { case (k, v) => (k, 2 * v) }
      .flatMap { case (k, v) => LMap((k, "short") -> -v, (k, "long") -> v) }
      .provenance("Signed exposures and doubled the investment")
      .collect { case (k, v) if v < 0 => (k._1, v / 2) }
      .map { case (k, v) => (k, -v) }
      .provenance("Return to the initial portfolio")

  println("Unevaluated ADT")
  println(lazystocks)

  println("Step-by-step evaluated ADT")
  // Trace execution
  println(lazystocks.trace)
}

