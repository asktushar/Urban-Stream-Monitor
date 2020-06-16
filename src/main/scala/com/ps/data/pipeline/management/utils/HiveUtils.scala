package com.ps.data.pipeline.management.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Calendar

class HiveUtils {

  var prepStmtTimeBasedCount: PreparedStatement = null
  var prepStmtLastRestartStatus: PreparedStatement = null
  var prepStmtMinTimeStamp: PreparedStatement = null
  var prepStmtHiveLog: PreparedStatement = null
  var prepStmtFirstEntry: PreparedStatement = null
  var connection: Connection =null

  def startConnection(connectionUrl: String):Boolean={
    connection = DriverManager.getConnection (connectionUrl)
    true
  }


  def getCurrentCount(countQuery: String): Long = {
    println("Staring to read count from Hive")

    val resultSet = connection.createStatement().executeQuery(countQuery)
    println("ResultSet is :- " + resultSet)

    val count = getCount(resultSet)
    count
  }

  def getTimeBasedCount(idealTime: String, appName: String, logSqlQuery: String): Long = {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val today = Calendar.getInstance()
    today.add(Calendar.MINUTE, -idealTime.toInt)
    val idealTimePartition = dateFormat.format(today.getTime)

    val dateFormatPartition = new SimpleDateFormat("yyyy-MM-dd")
    val hivePartition = dateFormatPartition.format(today.getTime)


    if (prepStmtTimeBasedCount == null)
      prepStmtTimeBasedCount = connection.prepareStatement(logSqlQuery)
    prepStmtTimeBasedCount.setString(1, appName)
    prepStmtTimeBasedCount.setString(2, idealTimePartition)
    prepStmtTimeBasedCount.setString(3, hivePartition)
    val resultSet = prepStmtTimeBasedCount.executeQuery()

    val count = getCount(resultSet)
    resultSet.close()
    count
  }

  def lastRestartStatus(sql: String, idealTime: Int, appName: String, status: String): Boolean = {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val today = Calendar.getInstance()
    today.add(Calendar.MINUTE, -idealTime.toInt)
    val idealTimePartition = dateFormat.format(today.getTime)

    val dateFormatPartition = new SimpleDateFormat("yyyy-MM-dd")
    val hivePartition = dateFormatPartition.format(today.getTime)

    if (prepStmtLastRestartStatus == null)
      prepStmtLastRestartStatus = connection.prepareStatement(sql)
    prepStmtLastRestartStatus.setString(1, appName)
    prepStmtLastRestartStatus.setString(2, status)
    prepStmtLastRestartStatus.setString(3, idealTimePartition)
    prepStmtLastRestartStatus.setString(4, hivePartition)

    val resultSet = prepStmtLastRestartStatus.executeQuery()

    val count = getCount(resultSet)
    resultSet.close()

    if (count >= 1) {
      true
    }
    else {
      false
    }
  }

  def getCount(resultSet: ResultSet): Long = {

    var count: Long = 0

    while (resultSet.next()) {
      count = resultSet.getLong("count")
      println("Count is + " + count)
    }
    count
  }

  def getMinTimeStamp(idealTime: String, appName: String, logSqlTimestampQuery: String): String = {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val today = Calendar.getInstance()
    today.add(Calendar.MINUTE, -(idealTime.toInt * 2))
    val idealTimePartition = dateFormat.format(today.getTime)

    val dateFormatPartition = new SimpleDateFormat("yyyy-MM-dd")
    val hivePartition = dateFormatPartition.format(today.getTime)

    if (prepStmtMinTimeStamp == null)
      prepStmtMinTimeStamp = connection.prepareStatement(logSqlTimestampQuery)

    prepStmtMinTimeStamp.setString(1, appName)
    prepStmtMinTimeStamp.setString(2, idealTimePartition)
    prepStmtMinTimeStamp.setString(3, hivePartition)
    val resultSet = prepStmtMinTimeStamp.executeQuery()

    var timestamp = ""
    while (resultSet.next()) {
      timestamp = resultSet.getString("log_datetime")
      println("timestamp is + " + timestamp)
    }
    resultSet.close()
    timestamp
  }

  def createLog(applicationId: String, status: String, currentCount: Long, appName: String, logDescription: String, insertLogQuery: String): Boolean = {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val today = Calendar.getInstance()
    val currentLogDateTime = dateFormat.format(today.getTime)

    val dateFormatForPartition = new SimpleDateFormat("yyyy-MM-dd")
    val partitionDate = dateFormatForPartition.format(today.getTime)

    if (prepStmtHiveLog == null)
      prepStmtHiveLog = connection.prepareStatement(insertLogQuery)
    //prepared Statement
    prepStmtHiveLog.setString(1, partitionDate)
    prepStmtHiveLog.setString(2, appName)
    prepStmtHiveLog.setString(3, applicationId)
    prepStmtHiveLog.setString(4, status)
    prepStmtHiveLog.setString(5, currentCount.toString)
    prepStmtHiveLog.setString(6, currentLogDateTime)
    prepStmtHiveLog.setString(7, logDescription)

    prepStmtHiveLog.executeUpdate()

    true
  }

  def checkFirstEntry(appName: String, getFirstLogQuery: String): Boolean = {

    if (prepStmtFirstEntry == null)
      prepStmtFirstEntry = connection.prepareStatement(getFirstLogQuery)

    prepStmtFirstEntry.setString(1, appName)

    val resultSet = prepStmtFirstEntry.executeQuery()

    val count = getCount(resultSet)
    resultSet.close()

    if (count > 0) {
      true
    }
    else {
      false
    }
  }

  def close(): Unit = {
    closePreparedStatement(prepStmtTimeBasedCount)
    closePreparedStatement(prepStmtLastRestartStatus)
    closePreparedStatement(prepStmtMinTimeStamp)
    closePreparedStatement(prepStmtHiveLog)
    closePreparedStatement(prepStmtFirstEntry)
    connection.close()
  }

  def closePreparedStatement(prepStmt: PreparedStatement): Unit = {
    if (prepStmt != null && !prepStmt.isClosed)
      prepStmt.close()
  }

}

