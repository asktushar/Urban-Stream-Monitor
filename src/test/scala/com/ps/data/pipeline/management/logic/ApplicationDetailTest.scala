package com.ps.data.pipeline.management.logic

import com.ps.data.pipeline.management.logic.ApplicationDetail
import com.ps.data.pipeline.management.utils.UnixUtils
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.runners.MockitoJUnitRunner
import org.mockito.{Mock, Mockito}

@RunWith(classOf[MockitoJUnitRunner])
class ApplicationDetailTest {

  @Mock var unixUtils: UnixUtils = _
  val applicationDetail= new ApplicationDetail

  @Test
  def TestGetApplicationIdAndStatus():Unit= {
    Mockito.when(unixUtils.getApplicationId("AppName")).thenReturn("applicationId_274727793237_32878")
    Mockito.when(unixUtils.getCurrentRunStatus("applicationId_274727793237_32878")).thenReturn("RUNNING")
    val applicationIdAndStatus = applicationDetail.getApplicationIdAndStatus("AppName", unixUtils)
    assert(applicationIdAndStatus.status == "RUNNING" && applicationIdAndStatus.applicationId == "applicationId_274727793237_32878")
  }

  @Test
  def TestGetApplicationIdAndStatusFail():Unit= {
    Mockito.when(unixUtils.getApplicationId("AppName")).thenReturn("")
    Mockito.when(unixUtils.getCurrentRunStatus("applicationId_274727793237_32878")).thenReturn("RUNNING")
    val applicationIdAndStatus = applicationDetail.getApplicationIdAndStatus("AppName", unixUtils)
    assert(applicationIdAndStatus.status == "Not Found Running" && applicationIdAndStatus.applicationId == "")
  }
}
