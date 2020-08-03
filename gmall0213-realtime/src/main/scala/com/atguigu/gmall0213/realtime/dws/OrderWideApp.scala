package com.atguigu.gmall0213.realtime.dws

import java.lang
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall0213.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall0213.realtime.util.{MyKafkaSink, MykafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.JedisCluster

import scala.collection.mutable.ListBuffer

/**
 * @author lcy
 * @create 2020-07-28 22:35
 */
object OrderWideApp {

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


//            val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoDstream.map(orderInfo=>(orderInfo.id,orderInfo))
//            val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))
//
//            val joinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream,4)
//

        //1.开窗 窗口大小 3个周期
        //2.join
        //3.去重

        //TODO 1.开窗
        val orderInfoWindowDstream: DStream[OrderInfo] = orderInfoDstream.window(Seconds(15),Seconds(5))
        val orderDetailWindowDstream: DStream[OrderDetail] = orderDetailDstream.window(Seconds(15),Seconds(5))

        //TODO 2.join
        val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoWindowDstream.map(orderInfo=>(orderInfo.id,orderInfo))
        val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailWindowDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))
        val joinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream,4)


        //TODO 3.去重
        //redis type?  set    key:  order_join:[orderId]    value : orderDetailId

        val orderWideDstream: DStream[OrderWide] = joinedDstream.mapPartitions { tupleItr =>
            val jedis: JedisCluster = RedisUtil.getJedisCluster()
            val orderWideList: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
            for ((orderId, (orderInfo, orderDetail)) <- tupleItr) {
                val key = "order_join:" + orderId
                val flag: lang.Long = jedis.sadd(key, orderDetail.id.toString)
                jedis.expire(key, 600)
                if (flag == 1L) {
                    //合并宽表
                    orderWideList.append(new OrderWide(orderInfo, orderDetail))
                }
            }
            orderWideList.toIterator
        }
//        orderWideDstream.print(1000)

        //TODO 思路：
        // 判断计算方式
        //如何判断最后一笔 ? 如果当前的一笔 个数*单价== 原始总金额-  Σ其他的明细（个数*单价）
        // 个数 单价  原始金额 可以从orderWide取到
        // 要从redis中取得累计值   Σ其他的明细（个数*单价）
        // redis    type?  string    key?  order_origin_sum:[order_id]  value? Σ其他的明细（个数*单价）
        //如果等式成立 说明该笔明细是最后一笔
        // 分摊计算公式 :减法公式  分摊金额= 实际付款金额-  Σ其他的明细的分摊金额  (减法，适用最后一笔明细）
        // 实际付款金额 在orderWide中，
        // 要从redis中取得  Σ其他的明细的分摊金额
        // redis    type?  string    key?  order_split_sum:[order_id]  value? Σ其他的明细的分摊金额
        //如果不成立
        // 分摊计算公式： 乘除法公式： 分摊金额= 实际付款金额 *(个数*单价) / 原始总金额   （乘除法，适用非最后一笔明细)
        //  所有计算要素都在 orderWide 中，直接计算即可
        // 分摊金额计算完成以后
        // 要赋给orderWide中分摊金额
        //  要本次计算的分摊金额 累计到redis    Σ其他的明细的分摊金额
        //   应付金额（单价*个数) 要累计到   Σ其他的明细（个数*单价）
        val orderWideWithDetail: DStream[OrderWide] = orderWideDstream.mapPartitions { orderWideItr =>
            val orderWideList: List[OrderWide] = orderWideItr.toList
            val jedis: JedisCluster = RedisUtil.getJedisCluster()
            for (orderWide <- orderWideList) {
                //从redis 中取出 该订单的其他明细（个数*单价）之和
                var otherSum: Double = 0D
                    if( jedis.get("order_origin_sum:" + orderWide.order_id) !=null){
                    otherSum= jedis.get("order_origin_sum:" + orderWide.order_id).toDouble
                }
                //求出该明细的原始金额（个数*单价）
                val amount: Double = orderWide.sku_num * orderWide.sku_price
                //用原始总金额减去该订单的其他明细之和，得到剩余的原始金额
                val remainAmount: Double = orderWide.original_total_amount - otherSum
                //判断是不是该订单的最后一笔明细   剩下的原始金额是否等于该明细的原始金额

                var final_amount: Double = 0D
                var other_detail_amount_sum: Double = 0D
                if (amount == remainAmount) {
                        //如果等式成立，则该笔订单详情是最后一笔
                        // 分摊计算公式 :减法公式  分摊金额= 实际付款金额-  Σ其他的明细的分摊金额  (减法，适用最后一笔明细）
                        // redis    type?  string    key?  order_split_sum:[order_id]  value? Σ其他的明细的分摊金额
                    if(jedis.get("order_split_sum:" + orderWide.order_id) !=null){
                        other_detail_amount_sum= jedis.get("order_split_sum:" + orderWide.order_id).toDouble
                    }
                    orderWide.final_detail_amount = Math.round((orderWide.final_total_amount - other_detail_amount_sum)*100D)/100D
                } else {
                    //该笔订单明细不是最后一笔
                    // 分摊计算公式： 乘除法公式： 分摊金额= 实际付款金额 *(个数*单价) / 原始总金额   （乘除法，适用非最后一笔明细)
                    final_amount= Math.round((orderWide.final_total_amount * amount / orderWide.original_total_amount)*100D)/100D
                    orderWide.final_detail_amount = final_amount
                }

                //将分摊金额累加后再存入redis中
                val other_amount_sum: Double = other_detail_amount_sum + final_amount
                jedis.setex("order_split_sum:" + orderWide.order_id, 600, other_amount_sum.toString)

                //将应付金额累加后存入redis中
                val other_origin_sum: Double = otherSum + amount
                jedis.setex("order_origin_sum:" + orderWide.order_id, 600, other_origin_sum.toString)

            }
            orderWideList.toIterator
        }

        orderWideWithDetail.persist(StorageLevel.MEMORY_ONLY)
        orderWideWithDetail.print(1000)




        val sparkSession: SparkSession = SparkSession.builder().appName("dws_order_wide_app").getOrCreate()
        import  sparkSession.implicits._
        orderWideWithDetail.foreachRDD{rdd=>

          rdd.cache()
          //存入clickhouse
            val df: DataFrame = rdd.toDF()
            df.write.mode(SaveMode.Append)
              .option("batchsize", "100")//批次写入
              .option("isolationLevel", "NONE") // 设置事务  clickhouse 不支持事务
              .option("numPartitions", "4") // 设置并发  4个分区
              .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
              .jdbc("jdbc:clickhouse://hadoop102:8123/test0213","order_wide_0213",new Properties())

            rdd.foreachPartition{orderWideItr=>
                val orderWideList: List[OrderWide] = orderWideItr.toList
                //写入kafka
                for (orderWide <- orderWideList ) {
                    MyKafkaSink.send("DWS_ORDER_WIDE",JSON.toJSONString(orderWide,new SerializeConfig(true)))
                }
            }

          //提交偏移量
            OffsetManager.saveOffset(orderInfoTopic,orderInfoGroupId,orderInfoOffsetRanges)
            OffsetManager.saveOffset(orderDetailTopic,orderDetailGroupId,orderDetailOffsetRanges)
        }

        ssc.start()
        ssc.awaitTermination()
    }

}
