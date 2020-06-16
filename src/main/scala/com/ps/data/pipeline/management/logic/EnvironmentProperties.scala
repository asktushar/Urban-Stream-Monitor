package com.ps.data.pipeline.management.logic

import java.io.{BufferedReader, FileInputStream, FileReader}
import java.util.Properties
import com.ps.data.pipeline.management.entity._


class EnvironmentProperties {


  def getEnvProperties(env: String): EnvProperties = {

    val path = s"../config/$env/envConfig.prop"
    println("Loading Env Properties from :" + path)
    val input = new FileInputStream(path)
    val prop = new Properties
    prop.load(input)

    val hiveConnectionUrl = prop.getProperty("hiveConnectionUrl")
    println("hiveConnectionUrl :" + hiveConnectionUrl)
    val phoenixConnectionUrl = prop.getProperty("PhoenixConnectionUrl")
    println("phoenixConnectionUrl :" + phoenixConnectionUrl)
    val insertLogQuery = prop.getProperty("insertLogQuery")
    val logSqlQuery = prop.getProperty("logSqlQuery")
    val timestampSqlQuery = prop.getProperty("timestampSqlQuery")
    val getFirstLogQuery = prop.getProperty("getFirstLogQuery")
    val lastRestartQuery = prop.getProperty("lastRestartQuery")

    input.close()

    EnvProperties(hiveConnectionUrl,phoenixConnectionUrl,insertLogQuery,logSqlQuery,timestampSqlQuery,getFirstLogQuery,lastRestartQuery)
  }

  def getAppProperties(appName: String, env: String): AppProperties = {

    var currentCountQuery: String = ""
    var currentCountQueryType: String = ""
    var sparkSubmitCommand: String = ""
    var idleTimeLimitToRestart: String = ""
    var idleTimeLimitToFail: String = ""

    val path = s"../config/$env/appConfig.prop"
    println("Reading App Properties from :" + path)

    val input = new FileReader(path)
    val br = new BufferedReader(input)
    var thisLine = ""
    thisLine = br.readLine()
    while (thisLine != null) {
      if (thisLine.contains(s"$appName")) {
        val a = thisLine.split(",")
        currentCountQueryType = a(1)
        currentCountQuery = a(2)
        println("Count Query :" + currentCountQuery)
        idleTimeLimitToRestart = a(3)
        println("Idle Time To Restart :" + idleTimeLimitToRestart)
        idleTimeLimitToFail = a(4)
        println("Idle Time To Fail :" + idleTimeLimitToFail)
        sparkSubmitCommand = s"sh ../appScripts/$appName.sh"
        println("Spark Submit Command :" + sparkSubmitCommand)
      }
      thisLine = br.readLine()

    }
    br.close()
    input.close()

    AppProperties(currentCountQuery,currentCountQueryType,sparkSubmitCommand,idleTimeLimitToRestart,idleTimeLimitToFail)

  }
}
