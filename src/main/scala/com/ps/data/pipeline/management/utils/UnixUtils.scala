package com.ps.data.pipeline.management.utils

import scala.sys.process._

class UnixUtils {


  def runUnixProcess(unixCommand: String): Int = {

    val result: Int = unixCommand !;
    println(result)
    result
  }

  def getCurrentRunStatus(applicationId: String): String = {
    val unixCommand = s"sh getApplicationStatus.sh $applicationId"
    println("Unix Command is" + unixCommand)
    val result = unixCommand lines_!;
    val status = result.mkString("")

    println(s"Status for $applicationId is $status")
    status
  }

  //TODO :: generic path in shell call
  def getApplicationId(appName: String): String = {

    val unixCommand = s"sh getApplicationId.sh $appName"
    println("Unix Command is " + unixCommand)
    val applicationId = unixCommand lines_!;
    println(s"Application Id for $appName is $applicationId")
    applicationId.mkString("")
  }

  def killApp(appId: String): Boolean = {
    val unixCommand = s"yarn application -kill $appId "
    println("Unix Command is" + unixCommand)
    val applicationId = unixCommand !;
    println(s"Application Id $appId is killed")
    if (applicationId == 0)
      true
    else
      false
  }

  def runSparkJob(sparkJob: String, env: String): Boolean = {
    val unixCommand = sparkJob + " " + env
    println("Unix Command is" + unixCommand)
    val jobRun: Int = unixCommand !;
    println("Submited Spark Job with " + unixCommand)
    if (jobRun == 0)
      true
    else
      false
  }

}
