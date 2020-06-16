package com.ps.data.pipeline.management.utils

import java.sql.{Connection, DriverManager}

class PhoenixUtils {

  def getCurrentCount(connectionUrl: String, countQuery: String): Long = {


    println("Opening Phoenix Connection")
    var count: Long = 0
    val conn = createConnection(connectionUrl)
    val stmt = conn.createStatement

    println(s"Executing count Query : $countQuery")
    val resultSet = stmt.executeQuery(countQuery)
    println("Resultset is :- " + resultSet)

    while (resultSet.next()) {
      count = resultSet.getLong("count")
      println("Count is + " + count)
    }
    resultSet.close()
    stmt.close()
    conn.close()
    println("Closing Phoenix connection")
    count

  }

  def createConnection(connectionUrl: String): Connection = {

    val conn = DriverManager.getConnection(connectionUrl)
    conn
  }

}
