package com.ps.data.pipeline.management.app

import com.ps.data.pipeline.management.logic.PipelineManagementProcessor

/**
  * This app will trigger Pipeline Management Suit App.
  * The app is designed to manage data pipelines with configurations provided.
  */

object PipelineManagementApp {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      throw new IllegalArgumentException("argument missing: <env> <appName>")
    }
    val env = args(0)
    val appName = args(1)

    println("<=====  Starting to run Pipeline Management App  =====>")
    println("<===== App Name : =====>" + appName)
    println("<===== Environment : =====>" + env)

    val pipelineManagementProcessor = new PipelineManagementProcessor

    pipelineManagementProcessor.start(appName, env)

    println("<=====  Completed to run Pipeline Management App  =====>")
  }
}

