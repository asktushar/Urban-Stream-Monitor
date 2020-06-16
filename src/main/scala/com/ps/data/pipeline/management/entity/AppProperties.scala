package com.ps.data.pipeline.management.entity

case class AppProperties(
                           currentCountQuery: String,
                           currentCountQueryType: String,
                           sparkSubmitCommand: String,
                           idleTimeLimitToRestart: String,
                           idleTimeLimitToFail: String
                         )