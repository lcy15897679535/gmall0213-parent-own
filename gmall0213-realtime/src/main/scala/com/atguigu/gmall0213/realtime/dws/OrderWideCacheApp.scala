package com.atguigu.gmall0213.realtime.dws

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall0213.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall0213.realtime.util.{MykafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.JedisCluster

import scala.collection.mutable.ListBuffer

/**
 * @author lcy
 * @create 2020-08-01 0:03
 */
object OrderWideCacheApp {

    def main(args: Array[String]): Unit = {

        //双流 订单主表，订单详情从表    偏移量都是双份
        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dws_order_wide_app")
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        val orderInfoGroupId = "dws_order_info_group"
        val orderInfoTopic = "DWD_ORDER_INFO"
        val orderDetailGroupId = "dws_order_detail_group"
        val orderDetailTopic = "DWD_ORDER_DETAIL"

        //1   从redis中读取偏移量   （启动执行一次）
        val orderInfoOffsetMapForKafka: Map[TopicPartition, Long] = OffsetManager.getOffset(orderInfoTopic, orderInfoGroupId)
        val orderDetailOffsetMapForKafka: Map[TopicPartition, Long] = OffsetManager.getOffset(orderDetailTopic, orderDetailGroupId)

        //2   把偏移量传递给kafka ，加载数据流（启动执行一次）
        var orderInfoRecordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
        if (orderInfoOffsetMapForKafka != null && orderInfoOffsetMapForKafka.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
            orderInfoRecordInputDstream = MykafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoOffsetMapForKafka, orderInfoGroupId)
        } else {
            orderInfoRecordInputDstream = MykafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoGroupId)
        }


        var orderDetailRecordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
        if (orderDetailOffsetMapForKafka != null && orderDetailOffsetMapForKafka.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
            orderDetailRecordInputDstream = MykafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailOffsetMapForKafka, orderDetailGroupId)
        } else {
            orderDetailRecordInputDstream = MykafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailGroupId)
        }


        //3   从流中获得本批次的 偏移量结束点（每批次执行一次）
        var orderInfoOffsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
        val orderInfoInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoRecordInputDstream.transform { rdd => //周期性在driver中执行
            orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        }

        var orderDetailOffsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
        val orderDetailInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailRecordInputDstream.transform { rdd => //周期性在driver中执行
            orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        }

        //提取数据
        // orderInfo
        val orderInfoDstream: DStream[OrderInfo] = orderInfoInputGetOffsetDstream.map { record =>
            val json: String = record.value()
            //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
            val orderInfo: OrderInfo = JSON.parseObject(json, classOf[OrderInfo])
            orderInfo
        }
        //orderDetail
        val orderDetailDstream: DStream[OrderDetail] = orderDetailInputGetOffsetDstream.map { record =>
            val json: String = record.value()
            val orderDetail: OrderDetail = JSON.parseObject(json, classOf[OrderDetail])
            orderDetail
        }


        // 方案二：  1 join   2.1 主表写缓存   2.2 主表查缓存   3.1 从表写缓存  3.2 从表读缓存
        //想join先变k-v
        val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoDstream.map(orderInfo=>(orderInfo.id,orderInfo))
        val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))

       val fullJoinedDstream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDstream.fullOuterJoin(orderDetailWithKeyDstream,4)

        val orderWideDstream: DStream[OrderWide] = fullJoinedDstream.flatMap { case (orderId, (orderInfoOpt, orderDetailOpt)) =>
            val jedis: JedisCluster = RedisUtil.getJedisCluster()
            val orderWideList: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
            if (orderInfoOpt != None) {
                val orderInfo: OrderInfo = orderInfoOpt.get
                if (orderDetailOpt != None) {
                    val orderDetail: OrderDetail = orderDetailOpt.get
                    val orderWide = new OrderWide(orderInfo, orderDetail) //join 成功产生一条宽表数据
                    orderWideList.append(orderWide)
                }

                //主表写缓存
                // redis  type?  string     key ?  order_info:0101    value? orderInfoJson  expire?:600
                //   实现 能够让从表方便的通过orderId 查到订单主表信息
                val orderInfoKey = "order_info:" + orderInfo.id
                val orderInfoStr: String = JSON.toJSONString(orderInfo, new SerializeConfig(true))
                jedis.setex(orderInfoKey, 600, orderInfoStr)

                //主表读缓存
                // 从表缓存如何设计：    实现主表能够方便的通过orderId
                //redis  type?   set   key?  order_detail:orderId value? 多个orderDetailJson     expire? 600
                val orderDetailKey = "order_detail:" + orderInfo.id
                val orderDetailStrSet: util.Set[String] = jedis.smembers(orderDetailKey)
                import scala.collection.JavaConverters._
                for (orderDetailStr <- orderDetailStrSet.asScala) {
                    val orderDetail: OrderDetail = JSON.parseObject(orderDetailStr, classOf[OrderDetail])
                    val orderWide = new OrderWide(orderInfo, orderDetail)
                    orderWideList.append(orderWide)
                }
            } else {
                val orderDetail: OrderDetail = orderDetailOpt.get
                //从表查询主表
                val orderInfoKey = "order_info:" + orderDetail.order_id
                val orderInfoStr: String = jedis.get(orderInfoKey)
                if (orderInfoStr != null && orderInfoStr.length > 0) {
                    val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
                    val orderWide = new OrderWide(orderInfo, orderDetail)
                    orderWideList.append(orderWide)
                } else {
                    //从表写缓存
                    // 从表缓存如何设计：    实现主表能够方便的通过orderId
                    //redis  type?   set   key?  order_detail:[order_id]  value? 多个orderDetailJson     expire? 600
                    val orderDetailKey = "order_detail:" + orderDetail.order_id
                    val orderDetailStr: String = JSON.toJSONString(orderDetail, new SerializeConfig(true))
                    jedis.sadd(orderDetailKey, orderDetailStr)
                    jedis.expire(orderDetailKey, 600)
                }
            }
            orderWideList.toIterator
        }
        orderWideDstream.print(1000)

        orderWideDstream.foreachRDD{rdd=>
            OffsetManager.saveOffset(orderInfoTopic,orderInfoGroupId,orderInfoOffsetRanges)
            OffsetManager.saveOffset(orderDetailTopic,orderDetailGroupId,orderDetailOffsetRanges)
        }

        ssc.start()
        ssc.awaitTermination()
    }

}
