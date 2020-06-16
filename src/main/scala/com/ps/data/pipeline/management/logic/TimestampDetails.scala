package com.ps.data.pipeline.management.logic

import com.ps.data.pipeline.management.utils._
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import com.ps.data.pipeline.management.entity.Timestamps

class TimestampDetails {

  def getTimestamps (phoenixCountQuery: String, idleTimeLimitToRestart: String, idleTimeLimitToFail: String,
                     phoenixConnectionUrl: String, hiveConnectionUrl: String, appName: String, logSqlQuery: String, hive: HiveUtils,
                     phoenix: PhoenixUtils = new PhoenixUtils): Timestamps = {

    val dateFormat = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss")
    val today = Calendar.getInstance ()

    val idleToFailTimeStamp = hive.getMinTimeStamp (idleTimeLimitToFail, appName, logSqlQuery)

    val idleToFailFormattedTS = if (! idleToFailTimeStamp.isEmpty)
      dateFormat.parse (idleToFailTimeStamp)
    else
      today.getTime


    val tsIdleToFailDiff: Long = TimeUnit.MILLISECONDS.toMinutes (today.getTime.getTime - idleToFailFormattedTS.getTime)

    val tsIdleToRestart: String = hive.getMinTimeStamp (idleTimeLimitToRestart, appName, logSqlQuery)
    val tsIdleToRestartFormatted = if (! tsIdleToRestart.isEmpty) dateFormat.parse (tsIdleToRestart) else today.getTime

    val tsIdleToRestartDiff: Long = TimeUnit.MILLISECONDS.toMinutes (today.getTime.getTime - tsIdleToRestartFormatted.getTime)

    Timestamps(tsIdleToFailDiff, tsIdleToRestartDiff)

  }

}
