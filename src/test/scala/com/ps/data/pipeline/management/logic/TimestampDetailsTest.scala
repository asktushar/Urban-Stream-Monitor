package com.ps.data.pipeline.management.logic

import com.ps.data.pipeline.management.logic.TimestampDetails
import com.ps.data.pipeline.management.utils.{HiveUtils, PhoenixUtils}
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.runners.MockitoJUnitRunner
import org.mockito.{Mock, Mockito}

@RunWith(classOf[MockitoJUnitRunner])
class TimestampDetailsTest {

  @Mock var hive: HiveUtils = _
  val timestampDetails= new TimestampDetails
  @Mock var phoenix: PhoenixUtils=_

  @Test
  def getTimestampsTest():Unit= {

    val phoenixCountQuery: String = ""
    val idleTimeLimitToRestart: String = "60"
    val idleTimeLimitToFail: String = "120"
    val phoenixConnectionUrl: String = ""
    val hiveConnectionUrl: String = ""
    val appName: String = ""
    val logSqlQuery: String = ""

    Mockito.when(hive.getMinTimeStamp(idleTimeLimitToFail, appName, logSqlQuery)).thenReturn("2020-10-10 12:01:23")
    Mockito.when(hive.getMinTimeStamp(idleTimeLimitToRestart, appName, logSqlQuery)).thenReturn("2020-10-10 12:01:23")
    val timeStampMap=timestampDetails.getTimestamps(phoenixCountQuery,idleTimeLimitToRestart,idleTimeLimitToFail,phoenixConnectionUrl,hiveConnectionUrl,appName,logSqlQuery,hive,phoenix)
    assert(!(timeStampMap.tsIdleToRestartDiff == ""))
  }

}


