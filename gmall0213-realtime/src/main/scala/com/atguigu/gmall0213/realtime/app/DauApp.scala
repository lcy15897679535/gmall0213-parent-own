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
import redis.clients.jedis.{Jedis, JedisCluster}

import scala.collection.mutable.ListBuffer

/**
 * @author lcy
 * @create 2020-07-17 15:42
 */
object DauApp {

    def main(args: Array[String]): Unit = {

        val sparkConf:SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
        val ssc = new StreamingContext(sparkConf,Seconds(5))
        val groupId = "dau_app"
        val topic = "GMALL_START0213"

        ///手动偏移量//////
        //1   从redis中读取偏移量   （启动执行一次）
        //2   把偏移量传递给kafka ，加载数据流（启动执行一次）

        //3   从流中获得本批次的 偏移量结束点（每批次执行一次）
        //4   把偏移量结束点 写入到redis中（每批次行一次）

        //TODO 1   从redis中读取偏移量   （启动执行一次）
        val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManager.getOffset(topic,groupId)
        //TODO 2   把偏移量传递给kafka ，加载数据流（启动执行一次）
        var recordInputDstream: InputDStream[ConsumerRecord[String, String]] =null
            if(offsetMapForKafka !=null && offsetMapForKafka.size>0){
                recordInputDstream = MykafkaUtil.getKafkaStream(topic,ssc,offsetMapForKafka,groupId)
            }else{
                recordInputDstream = MykafkaUtil.getKafkaStream(topic,ssc,groupId)
            }

        //TODO 3   从流中获得本批次的 偏移量结束点（每批次执行一次）
        var offsetRanges: Array[OffsetRange]=null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
        val inputGetOffsetDStream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform {
            rdd => //周期性在driver中执行
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
        }


        // TODO 去重操作
        //redis  1 快 2 管理性 3 代码可以变化
        //  redis去重
        // 去重  type:set  sadd     type:String setnx  既做了判断 又做了写入
        //set 当日的签到记录保存在一个set中   便于管理  整体查询速度快
        //string  把当天的签到记录 散列存储在redis   当量级巨大时 可以利用集群 做分布式

        // redis  type ? set   key?  dau:2020-07-18   value ? mid ...
        //sadd    既做了判断 又做了写入 成功1 失败0
        //       取出来 mid
        //         用mid 保存一个清单 （set）
        //       用sadd 执行
        //       判断返回值 1或0  1 保留数据 0 过滤掉
        //得到一个过滤后的Dstream

        val jsonObjectDStream: DStream[JSONObject] = inputGetOffsetDStream.map { record =>
            val jsonString: String = record.value()
            val jsonObject: JSONObject = JSON.parseObject(jsonString)
            jsonObject
        }
        val jsonObjFilteredDStream: DStream[JSONObject] = jsonObjectDStream.mapPartitions { jsonObjItr =>
            val jedis: JedisCluster = RedisUtil.getJedisCluster()

            //由于iterator 只能迭代一次，所以提取到list中
            val jsonObjectList: List[JSONObject] = jsonObjItr.toList
            println("过滤前" + jsonObjectList.size)
            val listBuffer = new ListBuffer[JSONObject]
            //遍历 list中的每个元素
            for (jsonObj <- jsonObjectList) {
                val mid: String = jsonObj.getJSONObject("common").getString("mid")
                val ts: lang.Long = jsonObj.getLong("ts")
                val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
                // redis  type ? set   key?  dau:2020-07-18   value ? mid ...
                val key = "dau:" + dt
                val flag: lang.Long = jedis.sadd(key, mid)

                // 判断返回值 1或0  1 保留数据 0 过滤掉
                if (flag == 1l) {
                    listBuffer.append(jsonObj)
                }
            }

            println("过滤后" + listBuffer.size)

            listBuffer.toIterator
        }
        jsonObjFilteredDStream.foreachRDD{
            rdd=>
//                rdd.foreach{println}

              rdd.foreachPartition{
                  jsonItr=>{
                      val jsonObjList: List[JSONObject] = jsonItr.toList
                      val dateFormat = new SimpleDateFormat("yyy-MM-dd HH:mm")
                      //转换数据格式（jsonObject）=>（dauInfo,id）
                      val dauWithIdList: List[(DauInfo, String)] = jsonObjList.map {
                          jsonObj =>
                              val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")

                              //获取日期 、小时、分钟
                              val ts: lang.Long = jsonObj.getLong("ts")
                              val dtHr: String = dateFormat.format(new Date(ts))
                              val dtHrArr: Array[String] = dtHr.split(" ")
                              val dt: String = dtHrArr(0)
                              val timeArr: Array[String] = dtHrArr(1).split(":")
                              val hr: String = timeArr(0)
                              val mi: String = timeArr(1)
                              val dauInfo = DauInfo(
                                  commonJsonObj.getString("mid"),
                                  commonJsonObj.getString("uid"),
                                  commonJsonObj.getString("ar"),
                                  commonJsonObj.getString("ch"),
                                  commonJsonObj.getString("vc"),
                                  dt, hr, mi, ts
                              )
                              (dauInfo, dauInfo.mid)
                      }
                      val format = new SimpleDateFormat("yyyyMMdd")
                      if(jsonObjList !=null && jsonObjList.size >0){
                          val ts: lang.Long = jsonObjList(0).getLong("ts")
                          val dt: String = format.format(new Date(ts))
                          MyEsUtil.bulkSave(dauWithIdList,"gmall_dau_info0213_"+dt)
                      }

                  }
              }

                OffsetManager.saveOffset(topic,groupId,offsetRanges)// 要在driver中执行 周期性 每批执行一次
        }







        ssc.start()
        ssc.awaitTermination()




    }

}
