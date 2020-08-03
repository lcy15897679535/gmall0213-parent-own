package com.atguigu.gmall0213.realtime.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0213.realtime.bean.{OrderInfo, UserState}
import com.atguigu.gmall0213.realtime.util.{MykafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
 * @author lcy
 * @create 2020-07-26 22:07
 */
object OrderInfoAppTest {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dwd_order_Info_app")
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val topic = "ODS_ORDER_INFO"
        val groupId = "dwd_order_Info_group"

        //从redis中读取偏移量
        val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManager.getOffset(topic,groupId)
        var recordInputDStream: InputDStream[ConsumerRecord[String, String]] =null
        //把偏移量传递给kafka
        if(offsetMapForKafka!=null && offsetMapForKafka.size>0){
            recordInputDStream = MykafkaUtil.getKafkaStream(topic,ssc,offsetMapForKafka,groupId)
        }else{
            recordInputDStream = MykafkaUtil.getKafkaStream(topic,ssc,groupId)
        }

        //3   从流中获得本批次的 偏移量结束点（每批次执行一次）
        var offsetRanges: Array[OffsetRange] =null
        //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
        val inputGetOffsetDStream: DStream[ConsumerRecord[String, String]] = recordInputDStream.transform {
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        }
        //业务操作  1 提取数据 2 分topic
        val orderInfoDstream: DStream[OrderInfo] = inputGetOffsetDStream.map {
            record =>
                val json: String = record.value()
                val orderInfo: OrderInfo = JSON.parseObject(json, classOf[OrderInfo])
                orderInfo.create_date = orderInfo.create_time.split(" ")(0)
                orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
                orderInfo
        }
        //周期性，分批次查询
        val orderInfoWithFlagDstream: DStream[OrderInfo] = orderInfoDstream.mapPartitions { orderInfoItr =>
            val orderList: List[OrderInfo] = orderInfoItr.toList
            if (orderList != null && orderList.size > 0) {
                val userIdList: List[Long] = orderList.map(orderInfo => orderInfo.user_id)
                // 1,2,3 ('1','2','3')
                val sql = "select USER_ID,IF_CONSUMED from USER_STATE0213 where USER_ID in('" + userIdList.mkString("','") + "')"
                val ifConsumedList: List[JSONObject] = PhoenixUtil.queryList(sql)
                val ifConsumedMap: Map[String, String] = ifConsumedList.map(jsonObj => (jsonObj.getString("USER_ID"), jsonObj.getString("IF_CONSUMED"))).toMap
                for (orderInfo <- orderList) {
                    val ifConsumed: String = ifConsumedMap.getOrElse(orderInfo.user_id.toString, "0")
                    if (ifConsumed == "1") { //表示不是首单
                        orderInfo.if_first_order = "0"
                    } else {
                        orderInfo.if_first_order = "1"
                    }

                }
            }
            orderList.toIterator
        }


        val orderInfoGroupByUserIdDstream: DStream[(Long, Iterable[OrderInfo])] = orderInfoWithFlagDstream.map(orderInfo=>(orderInfo.user_id,orderInfo)).groupByKey()
        val orderInfoRealWithFirstFlagDstream: DStream[OrderInfo] = orderInfoGroupByUserIdDstream.flatMap {
            case (user_id, orderInfoItr) =>
                val orderInfoList: List[OrderInfo] = orderInfoItr.toList
                if (orderInfoList != null && orderInfoList.size > 0) {
                    val orderInfoAny: OrderInfo = orderInfoList(0)
                    if (orderInfoList.size >= 2 && orderInfoAny.if_first_order == "1") {
                        val sortList: List[OrderInfo] = orderInfoList.sortWith((orderInfoLeft, orderInfoRight) =>
                            orderInfoLeft.create_time < orderInfoRight.create_time)
                        for (i <- 1 to sortList.size - 1) {
                            val orderInfoNotFirstThisBatch: OrderInfo = sortList(i)
                            orderInfoNotFirstThisBatch.if_first_order = "0"
                        }
                        sortList
                    } else {
                        orderInfoList
                    }
                } else {
                    orderInfoList
                }
        }

        orderInfoRealWithFirstFlagDstream.print(1000)
        //写入phoenix
        orderInfoRealWithFirstFlagDstream.foreachRDD{
            rdd=>
              val userStateRDD: RDD[UserState] = rdd.map(orderInfo=>UserState(orderInfo.user_id.toString,"1"))
                import org.apache.phoenix.spark._
                userStateRDD.saveToPhoenix("USER_STATE0213",
                    Seq("USER_ID", "IF_CONSUMED"),
                    new Configuration,
                    Some("hadoop102,hadoop103,hadoop104:2181"))
        }
        ssc.start()
        ssc.awaitTermination()

    }

}
