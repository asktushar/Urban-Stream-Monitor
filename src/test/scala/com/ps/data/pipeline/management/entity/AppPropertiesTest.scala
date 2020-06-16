package com.ps.data.pipeline.management.entity

import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.runners.MockitoJUnitRunner


@RunWith(classOf[MockitoJUnitRunner])
class AppPropertiesTest {

  val appProperties = AppProperties("countQuery","HIVE","spark-submit","10","20")

  @Test
  def TestCaseClass():Unit= {

    assert(appProperties.idleTimeLimitToRestart == "10")
  }

}
