package com.ps.data.pipeline.management.entity

import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.runners.MockitoJUnitRunner


@RunWith(classOf[MockitoJUnitRunner])
class TimestampsTest {

  val timestamps = Timestamps(5,10)

  @Test
  def TestCaseClass():Unit= {

    assert(timestamps.tsIdleToRestartDiff == 10)
  }

}
