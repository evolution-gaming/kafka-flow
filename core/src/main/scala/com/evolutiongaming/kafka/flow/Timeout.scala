package com.evolutiongaming.kafka.flow

import cats.effect.Concurrent
import cats.effect.Timer
import scala.concurrent.duration.FiniteDuration

trait Timeout[F[_]] {

  def timeout[A](fa: F[A]): F[A]

}
object Timeout {

  def apply[F[_]](implicit F: Timeout[F]): Timeout[F] = F

  def never[F[_]]: Timeout[F] = new Timeout[F] {
    def timeout[A](fa: F[A]) = fa
  }

  def of[F[_]: Concurrent: Timer](duration: FiniteDuration): Timeout[F] = new Timeout[F] {
    def timeout[A](fa: F[A]) = Concurrent.timeout(fa, duration)
  }

}
