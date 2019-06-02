package org.apache.spark.streaming

import org.apache.spark.FixedClock

object Clock {
  def getFixedClock(ssc: StreamingContext): FixedClock = {
    ssc.scheduler.clock.asInstanceOf[FixedClock]
  }
}
