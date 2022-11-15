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
    assert(subtractAndMultiply.run(0, 10) == -100)

    // 0 * 10 - 10 = -10
    assert(multiplyAndSubtract.run(0, 10) == -10)
  }

  test("Fold#handleErrorWith keeps the state from the first fold if used early") {

    type F[T] = Either[String, T]

    val add = Fold[F, Int, Int] { (s, a) => Right(s + a) }
    val fail = Fold[F, Int, Int] { (_, _) => Left("failed") }

    val addAndFail = add *> fail
    val failAndRecover = fail handleErrorWith[String] { (s, _) => Right(s) }

    val recoverEarly = add *> failAndRecover
    val recoverLate = addAndFail handleErrorWith[String] { (s, _) => Right(s) }

    // 1 + 2 = 3
    assertEquals(recoverEarly.run(1, 2), Right(3))

    // 1 = 1
    assertEquals(recoverLate.run(1, 2), Right(1))

  }


}