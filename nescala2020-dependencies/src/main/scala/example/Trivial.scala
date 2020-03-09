package example

object Trivial {
  sealed trait Expr
  case class Add(left: Expr, right: Expr) extends Expr
  case class Sub(left: Expr, right: Expr) extends Expr
  case class Num(n: Int) extends Expr

  val example = Sub(Num(4), Add(Num(3), Num(2)))

  def eval(e: Expr): Int = e match {
    case Num(n) => n
    case Add(left, right) => eval(left) + eval(right)
    case Sub(left, right) => eval(left) + eval(right)
  }
}
