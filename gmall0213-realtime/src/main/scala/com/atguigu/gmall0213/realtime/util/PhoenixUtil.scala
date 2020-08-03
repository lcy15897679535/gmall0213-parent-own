package com.atguigu.gmall0213.realtime.util

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}
import java.util.Properties

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
 * @author lcy
 * @create 2020-07-25 23:03
 */
object PhoenixUtil {

    def main(args: Array[String]): Unit = {
        val list: List[JSONObject] = queryList("select * from STUDENT")
        println(list)

    }

    def queryList(sql:String)={
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
        val resultList = new ListBuffer[JSONObject]
        val connection: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181")
        val statement: Statement = connection.createStatement()
//        println(sql)
        val rs: ResultSet = statement.executeQuery(sql)
        val md: ResultSetMetaData = rs.getMetaData
        while (rs.next()){
            val rowData = new JSONObject();
            for (i  <-1 to md.getColumnCount  ) {
                rowData.put(md.getColumnName(i), rs.getObject(i))
            }
            resultList+=rowData
        }

        statement.close()
        connection.close()
        resultList.toList
    }

}
