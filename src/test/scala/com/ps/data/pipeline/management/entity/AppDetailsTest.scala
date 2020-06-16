package com.ps.data.pipeline.management.entity

import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.runners.MockitoJUnitRunner


@RunWith(classOf[MockitoJUnitRunner])
class AppDetailsTest {

  val appDetails = AppDetails("application_1234","RUNNING")

  @Test
  def TestCaseClass():Unit= {

    assert(appDetails.status == "RUNNING")
  }

}
