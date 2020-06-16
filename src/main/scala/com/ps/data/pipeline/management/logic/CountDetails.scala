package com.ps.data.pipeline.management.logic

import com.ps.data.pipeline.management.utils._
import com.ps.data.pipeline.management.entity.CountMap

class CountDetails {

  def getCounts (lastRestartQuery: String, currentCountQueryType: String, countQuery: String, idleTimeLimitToRestart: String, idleTimeLimitToFail: String,
                 phoenixConnectionUrl: String, hiveConnectionUrl: String, appName: String, logSqlQuery: String, hive: HiveUtils,
                 phoenix: PhoenixUtils = new PhoenixUtils): CountMap = {

    val currentCount =
      if (currentCountQueryType == "Phoenix")
        phoenix.getCurrentCount (phoenixConnectionUrl, countQuery)
      else
        hive.getCurrentCount (countQuery)

    val restartCount = hive.getTimeBasedCount (idleTimeLimitToRestart, appName, logSqlQuery)
    val failCount = hive.getTimeBasedCount (idleTimeLimitToFail, appName, logSqlQuery)

    val lastRestartStatusRestart = hive.lastRestartStatus (lastRestartQuery, idleTimeLimitToRestart.toInt, appName, "KILLED_RESTART")
    val lastRestartStatusFail = hive.lastRestartStatus (lastRestartQuery, idleTimeLimitToFail.toInt, appName, "KILLED_TWS_FAIL")

    val countMap = CountMap(currentCount,restartCount,failCount,lastRestartStatusRestart,lastRestartStatusFail)

    countMap
  }
}
