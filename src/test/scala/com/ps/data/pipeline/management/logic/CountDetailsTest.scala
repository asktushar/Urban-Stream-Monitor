package com.ps.data.pipeline.management.logic

import com.ps.data.pipeline.management.logic.CountDetails
import com.ps.data.pipeline.management.utils.{HiveUtils, PhoenixUtils}
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.runners.MockitoJUnitRunner
import org.mockito.{Mock, Mockito}

@RunWith(classOf[MockitoJUnitRunner])
class CountDetailsTest {

  @Mock var hive: HiveUtils = _
  val countDetails= new CountDetails
  @Mock var phoenix: PhoenixUtils=_

  @Test
  def getCountsTest():Unit= {

    val lastRestartQuery: String = ""
    val currentCountQueryType: String = "Phoenix"
    val countQuery: String = ""
    val idleTimeLimitToRestart: String = "60"
    val idleTimeLimitToFail: String = "120"
    val phoenixConnectionUrl: String = ""
    val hiveConnectionUrl: String = ""
    val appName: String = ""
    val logSqlQuery: String = ""

    Mockito.when(phoenix.getCurrentCount(phoenixConnectionUrl,countQuery)).thenReturn(1000)
    Mockito.when(hive.getTimeBasedCount(idleTimeLimitToRestart, appName, logSqlQuery)).thenReturn(950)
    Mockito.when(hive.lastRestartStatus (lastRestartQuery, idleTimeLimitToRestart.toInt, appName, "KILLED_RESTART")).thenReturn(true)
    val countMap=countDetails.getCounts(lastRestartQuery,currentCountQueryType,countQuery,idleTimeLimitToRestart,idleTimeLimitToFail,phoenixConnectionUrl,hiveConnectionUrl,appName,logSqlQuery,hive,phoenix)
    assert(countMap.currentCount==1000)
  }

  @Test
  def getCountsTestHive():Unit= {

    val lastRestartQuery: String = ""
    val currentCountQueryType: String = "Hive"
    val countQuery: String = ""
    val idleTimeLimitToRestart: String = "60"
    val idleTimeLimitToFail: String = "120"
    val phoenixConnectionUrl: String = ""
    val hiveConnectionUrl: String = ""
    val appName: String = ""
    val logSqlQuery: String = ""

    Mockito.when(hive.getCurrentCount(countQuery)).thenReturn(1000)
    Mockito.when(hive.getTimeBasedCount(idleTimeLimitToRestart, appName, logSqlQuery)).thenReturn(950)
    Mockito.when(hive.lastRestartStatus (lastRestartQuery, idleTimeLimitToRestart.toInt, appName, "KILLED_RESTART")).thenReturn(true)
    val countMap=countDetails.getCounts(lastRestartQuery,currentCountQueryType,countQuery,idleTimeLimitToRestart,idleTimeLimitToFail,phoenixConnectionUrl,hiveConnectionUrl,appName,logSqlQuery,hive,phoenix)
    assert(countMap.currentCount==1000)
  }
}

