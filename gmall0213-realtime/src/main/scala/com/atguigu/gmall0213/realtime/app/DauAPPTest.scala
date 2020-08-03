package com.atguigu.gmall0213.realtime.app


import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0213.realtime.bean.DauInfo
import com.atguigu.gmall0213.realtime.util.{MyEsUtil, MykafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.JedisCluster

import scala.collection.mutable.ListBuffer


/**
 * @author lcy
 * @create 2020-07-19 16:28
 */
object DauAPPTest {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
        val ssc = new StreamingContext(sparkConf,Seconds(5))
        val topic = "GMALL_EVENT0213"
        val groupId = "dau_group"

        //TODO 1 读取redis的偏移量（启动执行一次）
        val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManager.getOffset(topic,groupId)

        var recordInputDStream: InputDStream[ConsumerRecord[String, String]] =null
        //TODO 2 把偏移量传递给kafka，加载数据流
        if(offsetMapForKafka!=null && offsetMapForKafka.size>0){
            //根据是否能取到偏移量来决定如何加载kafka 流
            recordInputDStream = MykafkaUtil.getKafkaStream(topic,ssc,offsetMapForKafka,groupId)
        }else{
            recordInputDStream = MykafkaUtil.getKafkaStream(topic,ssc,groupId)
        }

        //TODO 3 从流中获得本批次的 偏移量结束点（每批次执行一次）
        var offserRange: Array[OffsetRange] = null
        val inputGetOffsetDStream: DStream[ConsumerRecord[String, String]] = recordInputDStream.transform {
            rdd =>
                offserRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
        }


        //TODO 去重
        val jsonObjDstream: DStream[JSONObject] = inputGetOffsetDStream.map {
            record =>
              //提取出每条数据 json
                val json: String = record.value()
                //将json 变成jsonObject
                val jsonObject: JSONObject = JSON.parseObject(json)
                jsonObject
        }

        val jsonObjFilteredDStream: DStream[JSONObject] = jsonObjDstream.mapPartitions {
            jsonObjItr =>
                //iterator只能迭代一次
                val jsonObjList: List[JSONObject] = jsonObjItr.toList
                println("过滤前：" + jsonObjList.size)
                //创建一个可变list集合，存储mid
                val listBuffer = new ListBuffer[JSONObject]
                val jedis: JedisCluster = RedisUtil.getJedisCluster()
                for (jsonObj <- jsonObjList) {
                    val mid: String = jsonObj.getJSONObject("common").getString("mid")
                    val ts: lang.Long = jsonObj.getLong("ts")
                    val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))

                    val key = "dau:" + dt
                    val flag: lang.Long = jedis.sadd(key,mid)
                    //      判断返回值 1或0  1 保留数据 0 过滤掉
                    if (flag == 1l) {
                        listBuffer.append(jsonObj)
                    }

                }
                println("过滤后：" + listBuffer.size)
                listBuffer.toIterator
        }


        jsonObjFilteredDStream.foreachRDD{
            rdd=>
//                rdd.foreach(println)// 写入数据库的操作,在executor端执行
              //每个分区内的数据按批次处理
              rdd.foreachPartition{
                  jsonObjItr=>{
                      val jsonObjList: List[JSONObject] = jsonObjItr.toList
                      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm")
                      //将数据结构进行转换
                      val dauWithInfoList: List[(DauInfo, String)] = jsonObjList.map {
                          jsonObj =>
                              val ts: lang.Long = jsonObj.getLong("ts")
                              val dtHr: String = format.format(new Date(ts))
                              val dtHrArr: Array[String] = dtHr.split(" ")
                              val dt: String = dtHrArr(0)
                              val hr: String = dtHrArr(1).split(":")(0)
                              val mi: String = dtHrArr(1).split(":")(1)
                              val commonJson: JSONObject = jsonObj.getJSONObject("common")
                              val dauInfo = DauInfo(
                                  commonJson.getString("mid"),
                                  commonJson.getString("uid"),
                                  commonJson.getString("ar"),
                                  commonJson.getString("ch"),
                                  commonJson.getString("vc"),
                                  dt, hr, mi, ts
                              )
                              (dauInfo, dauInfo.mid)
                      }
                      val dateFormat = new SimpleDateFormat("yyyyMMdd")
                      if(jsonObjList !=null && jsonObjList.size >0){
                          val ts: lang.Long = jsonObjList(0).getLong("ts")
                          val dt: String = dateFormat.format(new Date(ts))
                          MyEsUtil.bulkSave(dauWithInfoList,"gmall_dau_info0213_"+dt)
                      }
                  }
              }


//                 要在driver中执行 周期性 每批执行一次
                OffsetManager.saveOffset(topic,groupId,offserRange)
        }




        ssc.start()
        ssc.awaitTermination()


    }

}
