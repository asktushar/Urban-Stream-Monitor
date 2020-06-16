package com.ps.data.pipeline.management.entity

import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.runners.MockitoJUnitRunner


@RunWith(classOf[MockitoJUnitRunner])
class CountMapTest {

  val countMap = CountMap(100,100,100,true,true)

  @Test
  def TestCaseClass():Unit= {

    assert(countMap.currentCount == 100)
  }

}
