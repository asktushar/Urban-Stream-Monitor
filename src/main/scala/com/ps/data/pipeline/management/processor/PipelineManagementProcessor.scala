package com.ps.data.pipeline.management.logic

import com.ps.data.pipeline.management.utils._

class PipelineManagementProcessor {

  def start(appName: String, env: String,
            property: EnvironmentProperties = new EnvironmentProperties,
            unix: UnixUtils = new UnixUtils,
            applicationDetail: ApplicationDetail = new ApplicationDetail,
            countDetails: CountDetails = new CountDetails,
            appsHandler: AppsHandler = new AppsHandler,
            timestampDetails: TimestampDetails = new TimestampDetails,
            hive: HiveUtils = new HiveUtils,
            phoenix: PhoenixUtils = new PhoenixUtils): Boolean = {

    // Getting Env Proverties from the Config Folder
    val envProperties = property.getEnvProperties(env)
    // Getting App Proverties from the Config Folder
    val appProperties = property.getAppProperties(appName, env)

    hive.startConnection(envProperties.hiveConnectionUrl)

    println("Getting application id and status for App from Yarn")
    val appDetails = applicationDetail.getApplicationIdAndStatus(appName, unix)
    val applicationId = appDetails.applicationId
    val status = appDetails.status
    println(s"ApplicationId :$applicationId , Status: $status")

    //Getting Current count, Restart count and Fail count
    println("Getting current Count")
    val countMap = countDetails.getCounts(envProperties.lastRestartQuery, appProperties.currentCountQueryType,
      appProperties.currentCountQuery, appProperties.idleTimeLimitToRestart, appProperties.idleTimeLimitToFail,
      envProperties.phoenixConnectionUrl, envProperties.hiveConnectionUrl, appName, envProperties.logSqlQuery, hive,phoenix)

    val currentCount: Long = countMap.currentCount
    val restartCount: Long = countMap.restartCount
    val failCount: Long = countMap.failCount
    val lastRestartStatusRestart: Boolean = countMap.lastRestartStatusRestart
    val lastRestartStatusFail: Boolean = countMap.lastRestartStatusFail

    println(s"Current count :$currentCount, " +
      s"Restart Count (First Count in last ${appProperties.idleTimeLimitToRestart} minutes) :$restartCount, " +
      s"Fail Count (First Count in last ${appProperties.idleTimeLimitToFail}) :$failCount, " +
      s"lastRestartStatusRestart :$lastRestartStatusRestart, " +
      s"lastRestartStatusFail :$lastRestartStatusFail")


    val timestamps = timestampDetails.getTimestamps(appProperties.currentCountQuery, appProperties.idleTimeLimitToRestart,
      appProperties.idleTimeLimitToFail, envProperties.phoenixConnectionUrl, envProperties.hiveConnectionUrl,
      appName, envProperties.timestampSqlQuery, hive,phoenix)

    val tsIdleToFailDiff = timestamps.tsIdleToFailDiff
    val tsIdleToRestartDiff = timestamps.tsIdleToRestartDiff

    //Count Check
    if (
      (currentCount <= failCount) &&
        currentCount > 0 &&
        tsIdleToFailDiff >= appProperties.idleTimeLimitToFail.toLong &&
        !lastRestartStatusFail
    ) {
      println("=====> Application crosses Idle time to fail threshold without count increase. So will restart the application and will report failure to Scheduling Tool.")
      if (applicationId != "") {
        println(s"Killing application $applicationId")
        appsHandler.killJob(applicationId, unix)
        println(s"Killed application $applicationId")
        val message = s"Killed Job ($applicationId)"
        println("Logging status to hive")
        hive.createLog(applicationId, "KILLED_TWS_FAIL", currentCount, appName, message, envProperties.insertLogQuery)
      }
      val message = s"JOB RESTARTED. REPORTED TWS FAILURE. NEW APPLICATION ID - "
      jobHandler(appName, appProperties.sparkSubmitCommand, env,message, unix,applicationDetail)

      //Exit (1) to FAIL TWS
      println("Exiting with failure. Exit Code = 1")
      sys.exit(1)
    }


    if (
      currentCount <= restartCount &&
        currentCount > 0 &&
        tsIdleToRestartDiff >= appProperties.idleTimeLimitToRestart.toLong &&
        !lastRestartStatusRestart
    ) {
      println("====> Application crosses Idle time to restart threshold without count increase. So will restart the application and will report success to TWS.")
      if (applicationId != "") {
        println(s"Killing application $applicationId")
        appsHandler.killJob(applicationId, unix)
        println(s"Killed application $applicationId")
        val message = s"Killed Job ($applicationId)"
        println("Logging status to hive")
        hive.createLog(applicationId, "KILLED_RESTART", currentCount, appName, message, envProperties.insertLogQuery)
      }

      val message = s"JOB RESTARTED. NEW APPLICATION ID - "
      jobHandler(appName, appProperties.sparkSubmitCommand, env,message, unix,applicationDetail)

      println("Exiting with failure. Exit Code = 0")
      sys.exit(0)
    }

    //App Status Check
    if (status == "RUNNING") {
      println("====> Application is running and with in idelt time to start and idle tile to fail thresholds.")
      val logDescription = "APP IS RUNNING. JUST LOGGING"
      println("Logging status to hive")
      hive.createLog(applicationId, status, currentCount, appName, logDescription, envProperties.insertLogQuery)
    } else {
      //No Log Entry in Table

      val firstEntry = hive.checkFirstEntry(appName, envProperties.getFirstLogQuery)
      if (!firstEntry) {
        println("====> No log entry in hive, Assuming Day 0 execution of application.")

        val message = s"STARTING JOB FOR THE FIRST TIME. APPLICATION ID - "
        jobHandler(appName, appProperties.sparkSubmitCommand, env,message, unix,applicationDetail)

        println("Exiting with success. Exit Code = 0")
        sys.exit(0)
      }

      if (applicationId == "") { //no job running

        println("====> No application found running, starting the application.")

        val message = s"JOB STARTED. APPLICATION ID - "
        jobHandler(appName, appProperties.sparkSubmitCommand, env,message, unix,applicationDetail)
        println("Exiting with success. Exit Code = 0")
        sys.exit(0)
      }

      //Status like halt/stalled/accepted
      println("====> Application found but in status other then running/failed/killed. It could be halted/stalled or accepted, So will do nothing.")
      val logDescription = "NO APPLICATION FOUND IN RUNNING STATE"
      println("Logging status to hive")
      hive.createLog(applicationId, status, currentCount, appName, logDescription, envProperties.insertLogQuery)
      println("Exiting with success. Exit Code = 0")
      sys.exit(0)

      println("Closing hive connections and statements.")
      hive.close()
    }

    def jobHandler(appName :String, sparkSubmitCommand: String, env :String,logDescription: String, unix: UnixUtils, applicationDetail: ApplicationDetail): Unit = {
      println("Starting Application")
      val appDetails = appsHandler.startApp(appName, appProperties.sparkSubmitCommand, env, unix,applicationDetail)
      val newApplicationId = appDetails.applicationId
      val newStatus = appDetails.status
      println(s"Started Application $newApplicationId, Restarted Status:$newStatus")
      println("Logging status to hive")
      hive.createLog(newApplicationId, newStatus, currentCount, appName, logDescription+newApplicationId, envProperties.insertLogQuery)
      println("Completed to log status to hive")
    }

    true
  }
}
