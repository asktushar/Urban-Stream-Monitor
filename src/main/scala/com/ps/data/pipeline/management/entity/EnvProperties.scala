package com.ps.data.pipeline.management.entity

case class EnvProperties(
                           hiveConnectionUrl: String,
                           phoenixConnectionUrl: String,
                           insertLogQuery: String,
                           logSqlQuery: String,
                           timestampSqlQuery: String,
                           getFirstLogQuery: String,
                           lastRestartQuery: String
                         )
