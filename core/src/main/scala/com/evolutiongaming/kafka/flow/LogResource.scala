package com.evolutiongaming.kafka.flow

import cats.FlatMap
import cats.effect.Resource
import cats.syntax.all._
import com.evolutiongaming.catshelper.{Log, LogOf}

object LogResource {

  def apply[F[_] : FlatMap : LogOf](source: Class[_], prefix: String): Resource[F, Log[F]] = {
    val result = for {
      log0 <- LogOf[F].apply(source)
      log   = log0.prefixed(prefix)
      _    <- log.info("starting")
    } yield {
      val release = log.info("stopping")
      (log, release)
    }
    Resource(result)
  }
}
