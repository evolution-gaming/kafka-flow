package com.evolutiongaming.kafka.flow

/** Extract a common prefix from `A` that is suitable for being logged in order to correlate a specific log message with
  * `A`
  */
trait LogPrefix[A] {
  def extract(value: A): String
}

object LogPrefix {
  def apply[A](implicit instance: LogPrefix[A]): LogPrefix[A] = instance

  def function[A](f: A => String): LogPrefix[A] = value => f(value)

  implicit val stringPrefix: LogPrefix[String] = function(identity)
}
