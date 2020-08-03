package com.atguigu.gmall0213.realtime.util

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author lcy
 * @create 2020-07-17 16:21
 */
object PropertiesUtil {

    def main(args: Array[String]): Unit = {
        val properties: Properties = PropertiesUtil.load("config.properties")

        println(properties.getProperty("kafka.broker.list"))
    }

    def load(propertieName:String): Properties ={
        val prop=new Properties();
        prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
        prop
    }

}
