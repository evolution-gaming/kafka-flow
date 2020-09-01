package com.evolutiongaming.kafka.flow

import cats.Id
import munit.FunSuite

class FoldSpec extends FunSuite {

  test("Fold#productR combines two folds correctly") {
    val subtract = Fold[Id, Int, Int] { (s, a) => s - a }
    val multiply = Fold[Id, Int, Int] { (s, a) => s * a }

    val subtractAndMultiply = subtract *> multiply
    val multiplyAndSubtract = multiply *> subtract

    // (0 - 10) * 10 = -100
    assert(subtractAndMultiply(0, 10) == -100)

    // 0 * 10 - 10 = -10
    assert(multiplyAndSubtract(0, 10) == -10)
  }


}