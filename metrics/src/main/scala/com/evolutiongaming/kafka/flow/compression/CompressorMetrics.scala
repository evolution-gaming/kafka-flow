package com.evolutiongaming.kafka.flow.compression

import cats.Monad
import cats.effect.Resource
import cats.implicits._
import com.evolutiongaming.kafka.flow.metrics.Metrics
import com.evolutiongaming.kafka.flow.persistence.compression.Compressor
import com.evolutiongaming.smetrics._
import scodec.bits.ByteVector

object CompressorMetrics {
  private[flow] class CompressorMetricsOf[F[_]: Monad](
    rawBytes: LabelValues.`1`[Summary[F]],
    compressedBytes: LabelValues.`1`[Summary[F]]
  ) {
    def make(component: String): Metrics[Compressor[F]] =
      new Metrics[Compressor[F]] {
        override def withMetrics(compressor: Compressor[F]): Compressor[F] = new Compressor[F] {
          override def to(payload: ByteVector): F[ByteVector] =
            for {
              _ <- rawBytes.labels(component).observe(payload.size.toDouble)
              compressed <- compressor.to(payload)
              _ <- compressedBytes.labels(component).observe(compressed.size.toDouble)
            } yield compressed

          override def from(bytes: ByteVector): F[ByteVector] =
            compressor.from(bytes)
        }
      }
  }

  def compressorMetricsOf[F[_]: Monad](registry: CollectorRegistry[F]): Resource[F, CompressorMetricsOf[F]] = {
    val rawBytesSummary = registry.summary(
      name = "compressor_raw_bytes_total",
      help = "Payload size in bytes before compression",
      quantiles = Quantiles(Quantile(value = 0.9, error = 0.05), Quantile(value = 0.99, error = 0.005)),
      labels = LabelNames("component")
    )

    val compressedSummary = registry.summary(
      name = "compressor_compressed_bytes_total",
      help = "Payload size in bytes after compression",
      quantiles = Quantiles(Quantile(value = 0.9, error = 0.05), Quantile(value = 0.99, error = 0.005)),
      labels = LabelNames("component")
    )

    for {
      rawBytesSummary <- rawBytesSummary
      compressedSummary <- compressedSummary
    } yield new CompressorMetricsOf[F](rawBytesSummary, compressedSummary)
  }
}
