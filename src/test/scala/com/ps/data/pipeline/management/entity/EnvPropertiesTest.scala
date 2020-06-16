package com.ps.data.pipeline.management.entity

import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.runners.MockitoJUnitRunner


@RunWith(classOf[MockitoJUnitRunner])
class EnvPropertiesTest {

  val properties = EnvProperties("hiveConnectionUrl","phoenixConnectionUrl","insertLogQuery",
    "logSqlQuery","timestampSqlQuery","getFirstLogQuery","lastRestartQuery")

  @Test
  def TestCaseClass():Unit= {

    assert(properties.getFirstLogQuery == "getFirstLogQuery")
  }

}
