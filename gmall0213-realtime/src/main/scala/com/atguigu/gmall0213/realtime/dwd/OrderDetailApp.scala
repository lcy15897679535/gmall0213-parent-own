package com.atguigu.gmall0213.realtime.dwd

import java.lang

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0213.realtime.bean.OrderDetail
import com.atguigu.gmall0213.realtime.util.{MyKafkaSink, MykafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
 * @author lcy
 * @create 2020-07-27 21:54
 */
object OrderDetailApp {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dwd_order_detail_app")
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val topic = "ODS_ORDER_DETAIL"
        val groupId = "dwd_order_detail_group"

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

        val orderDetailDstream: DStream[OrderDetail] = inputGetOffsetDStream.map { record =>
            val json: String = record.value()
            val orderDetail: OrderDetail = JSON.parseObject(json, classOf[OrderDetail])
            orderDetail
        }


        //////////////关联商品维度信息/////////////////////
        val OrderDetailWithSkuDstream: DStream[OrderDetail] = orderDetailDstream.mapPartitions { orderDetailItr =>
            val orderDetailList: List[OrderDetail] = orderDetailItr.toList
            val skuIdList: List[Long] = orderDetailList.map(orderDetail => orderDetail.sku_id)

            val sql = "select ID,SPU_ID,TM_ID,CATEGORY3_ID,SPU_NAME,TM_NAME,CATEGORY3_NAME " +
              " from GMALL0213_SKU_INFO where ID in ('" + skuIdList.mkString("','") + "')"
            val skuInfoObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
            val skuInfoObjMap: Map[lang.Long, JSONObject] = skuInfoObjList.map(skuInfoObj => (skuInfoObj.getLong("ID"), skuInfoObj)).toMap

            val OrderDetailWithSku_InfoList: List[OrderDetail] = orderDetailList.map { orderDetail =>
                val skuInfoJsonObj: JSONObject = skuInfoObjMap.getOrElse(orderDetail.sku_id, null)
                if (skuInfoJsonObj != null) {
                    orderDetail.spu_id = skuInfoJsonObj.getLong("SPU_ID")
                    orderDetail.tm_id = skuInfoJsonObj.getLong("TM_ID")
                    orderDetail.category3_id = skuInfoJsonObj.getLong("CATEGORY3_ID")
                    orderDetail.spu_name = skuInfoJsonObj.getString("SPU_NAME")
                    orderDetail.tm_name = skuInfoJsonObj.getString("TM_NAME")
                    orderDetail.category3_name = skuInfoJsonObj.getString("CATEGORY3_NAME")
                }
                orderDetail
            }
            OrderDetailWithSku_InfoList.toIterator
        }


        OrderDetailWithSkuDstream.print(1000)


        //写入操作
        // 1  存储olap  用户分析    可选
        // 2  推kafka 进入下一层处理   可选
        // 3 保存偏移量
        OrderDetailWithSkuDstream.foreachRDD{rdd=>

            rdd.foreachPartition{orderDetailItr=>
                val orderDetailList: List[OrderDetail] = orderDetailItr.toList
                val orderDetailMapList: List[(Long, OrderDetail)] = orderDetailList.map(orderDetail=>(orderDetail.id,orderDetail))

                //写入kafka
                for ((id,orderDetail )<- orderDetailMapList ) {
                    MyKafkaSink.send("DWD_ORDER_DETAIL",id.toString,JSON.toJSONString(orderDetail,new SerializeConfig(true)))
                }
            }
            //保存偏移量
            OffsetManager.saveOffset(topic,groupId,offsetRanges)
        }

        ssc.start()
        ssc.awaitTermination()

    }

}
