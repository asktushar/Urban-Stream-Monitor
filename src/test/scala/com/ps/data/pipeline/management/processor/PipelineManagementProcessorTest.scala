package com.ps.data.pipeline.management.processor

import com.ps.data.pipeline.management.entity.{AppDetails, _}
import com.ps.data.pipeline.management.logic._
import com.ps.data.pipeline.management.utils.{HiveUtils, PhoenixUtils, UnixUtils}
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.runners.MockitoJUnitRunner
import org.mockito.{Mock, Mockito}

@RunWith(classOf[MockitoJUnitRunner])
class PipelineManagementProcessorTest {

  @Mock var environmentProperties: EnvironmentProperties = _
  @Mock var unixUtils: UnixUtils = _
  @Mock var applicationDetail: ApplicationDetail = _
  @Mock var countDetails: CountDetails = _
  @Mock var appsHandler: AppsHandler = _
  @Mock var timestampDetails: TimestampDetails = _
  @Mock var hiveUtils: HiveUtils = _
  @Mock var phoenixUtils: PhoenixUtils = _

  val lastRestartQuery: String = ""
  val currentCountQueryType: String = "Phoenix"
  val countQuery: String = ""
  val idleTimeLimitToRestart: String = "60"
  val idleTimeLimitToFail: String = "120"
  val phoenixConnectionUrl: String = ""
  val hiveConnectionUrl: String = ""
  val appName: String = ""
  val logSqlQuery: String = ""
  val phoenixCountQuery: String = ""

  val pipelineManagementProcessor= new PipelineManagementProcessor

  val properties = EnvProperties("hiveConnectionUrl","phoenixConnectionUrl","insertLogQuery",
    "logSqlQuery","timestampSqlQuery","getFirstLogQuery","lastRestartQuery")

  val appProperties = AppProperties( "currentCountQuery","currentCountQueryType","sparkSubmitCommand",
    "60","90")

  val appDetails= AppDetails("applicationId_274727793237_32878","RUNNING")
  val appDetails1= AppDetails("","")

  val countMap = CountMap(100,100,100,true,true)

  val timestamps = Timestamps(5,10)


  @Test
  def TestStart():Unit= {
    Mockito.when(environmentProperties.getEnvProperties("dev")).thenReturn(properties)
    Mockito.when(environmentProperties.getAppProperties("AppName","dev")).thenReturn(appProperties)
    Mockito.when(hiveUtils.startConnection(any(classOf[String]))).thenReturn(true)
    Mockito.when(applicationDetail.getApplicationIdAndStatus("AppName",unixUtils)).thenReturn(appDetails)
    Mockito.when(countDetails.getCounts(any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[HiveUtils]),any(classOf[PhoenixUtils]))).thenReturn(countMap)

    Mockito.when(timestampDetails.getTimestamps(any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[HiveUtils]),any(classOf[PhoenixUtils]))).thenReturn(timestamps)

    Mockito.when(appsHandler.killJob(any(classOf[String]), any(classOf[UnixUtils]))).thenReturn(true)
    Mockito.when(hiveUtils.createLog(any(classOf[String]), any(classOf[String]), any(classOf[Long]), any(classOf[String]), any(classOf[String]), any(classOf[String]))).thenReturn(true)
    Mockito.when(appsHandler.startApp(any(classOf[String]), any(classOf[String]), any(classOf[String]), any(classOf[UnixUtils]),any(classOf[ApplicationDetail]))).thenReturn(appDetails)


    val pipelineManagementProcessorStartStatus = pipelineManagementProcessor.start("AppName", "dev",
      environmentProperties,unixUtils,applicationDetail,countDetails,appsHandler,timestampDetails,hiveUtils,phoenixUtils)

    assert(pipelineManagementProcessorStartStatus)
  }

  @Test
  def TestStartNotRunning():Unit= {
    Mockito.when(environmentProperties.getEnvProperties("dev")).thenReturn(properties)
    Mockito.when(environmentProperties.getAppProperties("AppName","dev")).thenReturn(appProperties)
    Mockito.when(hiveUtils.startConnection(any(classOf[String]))).thenReturn(true)
    Mockito.when(applicationDetail.getApplicationIdAndStatus("AppName",unixUtils)).thenReturn(appDetails1)
    Mockito.when(countDetails.getCounts(any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[HiveUtils]),any(classOf[PhoenixUtils]))).thenReturn(countMap)

    Mockito.when(timestampDetails.getTimestamps(any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[String]),any(classOf[HiveUtils]),any(classOf[PhoenixUtils]))).thenReturn(timestamps)

    Mockito.when(appsHandler.killJob(any(classOf[String]), any(classOf[UnixUtils]))).thenReturn(true)
    Mockito.when(hiveUtils.createLog(any(classOf[String]), any(classOf[String]), any(classOf[Long]), any(classOf[String]), any(classOf[String]), any(classOf[String]))).thenReturn(true)
    Mockito.when(appsHandler.startApp(any(classOf[String]), any(classOf[String]), any(classOf[String]), any(classOf[UnixUtils]),any(classOf[ApplicationDetail]))).thenReturn(appDetails1)


    val pipelineManagementProcessorStartStatus = pipelineManagementProcessor.start("AppName", "dev",
      environmentProperties,unixUtils,applicationDetail,countDetails,appsHandler,timestampDetails,hiveUtils,phoenixUtils)

    assert(pipelineManagementProcessorStartStatus)
  }



}
