package com.atguigu.gmall0213.realtime.util

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
 * @author lcy
 * @create 2020-07-25 23:03
 */
object MySqlUtil {

    def main(args: Array[String]): Unit = {
        val list: List[JSONObject] = queryList("select * from offset_0213")
        println(list)

    }

    def queryList(sql:String)={
        Class.forName("com.mysql.jdbc.Driver")
        val resultList = new ListBuffer[JSONObject]
        val connection: Connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall0213_rs?characterEncoding=utf-8&useSSL=false","root","123456")
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
