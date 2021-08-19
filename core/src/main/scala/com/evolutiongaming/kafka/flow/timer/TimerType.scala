package com.evolutiongaming.kafka.flow.timer

private[timer] sealed trait TimerType
private[timer] object TimerType {
  case object Clock extends TimerType
  case object Watermark extends TimerType
  case object Offset extends TimerType
}
