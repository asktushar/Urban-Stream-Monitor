package com.ps.data.pipeline.management.logic

import com.ps.data.pipeline.management.entity.AppDetails
import com.ps.data.pipeline.management.utils._
class AppsHandler {

  def killJob (applicationId: String, unix: UnixUtils): Boolean = {
    val killedSuccessfully = unix.killApp (applicationId)
    if (! killedSuccessfully) {
      throw new IllegalArgumentException (s"Tried Killing Application - $applicationId, But Failed")
    }
    true
  }

  def startApp (appName: String, sparkSubmitCommand: String, env: String, unix: UnixUtils, applicationDetail: ApplicationDetail = new ApplicationDetail): AppDetails = {
    unix.runSparkJob (sparkSubmitCommand, env)
    val appDetails = applicationDetail.getApplicationIdAndStatus (appName, unix)
    appDetails
  }

}
