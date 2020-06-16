package com.ps.data.pipeline.management.logic

import com.ps.data.pipeline.management.utils._
import com.ps.data.pipeline.management.entity._

class ApplicationDetail {
  def getApplicationIdAndStatus(appName: String, unix: UnixUtils): AppDetails = {
    println("Getting applicationId " + appName)
    val applicationId = unix.getApplicationId(appName)
    println("ApplicationId for " + appName + " Is :" + applicationId)

    // Getting STATUS if exists
    println("Getting current run status for " + appName)
    val status: String = if (applicationId != "") {
      println("Getting current run status for " + applicationId)
      unix.getCurrentRunStatus(applicationId)
    }
    else {
      println(appName + " Was not running. Status is null")
      "Not Found Running"
    }

    AppDetails(applicationId, status)

  }
}
