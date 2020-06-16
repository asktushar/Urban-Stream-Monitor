package com.ps.data.pipeline.management.logic

import com.ps.data.pipeline.management.entity.AppDetails
import com.ps.data.pipeline.management.logic.{ApplicationDetail, AppsHandler}
import com.ps.data.pipeline.management.utils.UnixUtils
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.runners.MockitoJUnitRunner
import org.mockito.{Mock, Mockito}

@RunWith(classOf[MockitoJUnitRunner])
class AppsHandlerTest {

  @Mock var unixUtils: UnixUtils = _
  val appsHandler= new AppsHandler
  @Mock var applicationDetail : ApplicationDetail=_

  val appDetails  = AppDetails("applicationId_274727793237_32878","RUNNING")

  @Test
  def KillJobTest():Unit= {
    Mockito.when(unixUtils.killApp("applicationId_274727793237_32878")).thenReturn(true)
    assert(appsHandler.killJob("applicationId_274727793237_32878",unixUtils).equals(true))
  }

  @Test
  def TestGetApplicationIdAndStatusFail():Unit= {
    Mockito.when(unixUtils.runSparkJob("spark-submit ","dev")).thenReturn(true)
    Mockito.when(applicationDetail.getApplicationIdAndStatus("AppName",unixUtils)).thenReturn(appDetails)
    val  app  = appsHandler.startApp("AppName","spark-submit ","dev",unixUtils,applicationDetail)
    assert(app.applicationId=="applicationId_274727793237_32878" && app.status=="RUNNING")
  }
}
