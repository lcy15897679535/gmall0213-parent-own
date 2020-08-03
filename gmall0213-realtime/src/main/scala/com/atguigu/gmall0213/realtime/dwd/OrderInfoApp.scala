package com.atguigu.gmall0213.realtime.dwd

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0213.realtime.bean.{OrderInfo, UserState}
import com.atguigu.gmall0213.realtime.util.{MyEsUtil, MyKafkaSink, MykafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
 * @author lcy
 * @create 2020-07-25 12:37
 */
object OrderInfoApp {

    def main(args: Array[String]): Unit = {
        //  偏移量管理？    精确一次消费？ kafka作为最后存储端 无法保证幂等性 只能做“至少一次消费”
        //  手动后置偏移量必须保证  防止宕机丢失数据
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
        val orderInfoDStream: DStream[OrderInfo] = inputGetOffsetDStream.map {
            record =>
                val jsonString: String = record.value()
                //订单处理  脱敏  换成特殊字符 或者直接去掉   这里转换成更方便操作的专用样例类
                val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
                val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
                orderInfo.create_date = createTimeArr(0)
                orderInfo.create_hour = createTimeArr(1).split(":")(0)
                orderInfo
        }

        //按照周期+分区 组成大sql查询，减少查询次数
        val orderInfoWithFlagDstream: DStream[OrderInfo] = orderInfoDStream.mapPartitions { orderInfoItr =>
            val orderInfoList: List[OrderInfo] = orderInfoItr.toList
            //判断每个分区内的数据是否为空
            if (orderInfoList != null && orderInfoList.size > 0) {
                //提取出数据中的user_id
                val userIdList: List[Long] = orderInfoList.map(orderInfo => orderInfo.user_id)
                val sql = "select USER_ID,IF_CONSUMED from USER_STATE0213 where USER_ID in ('" + userIdList.mkString("','") + "')"
                val ifConsumedList: List[JSONObject] = PhoenixUtil.queryList(sql)
                val ifConsumedMap: Map[String, String] = ifConsumedList.map(jsonObj => (jsonObj.getString("USER_ID"), jsonObj.getString("IF_CONSUMED"))).toMap

                for (orderInfo <- orderInfoList) {
                    //判断从hbase中查询的user_id 在这批次的分区内是否存在，存在则取出来做后续判断
                    val ifConsumed: String = ifConsumedMap.getOrElse(orderInfo.user_id.toString, "0")
                    if (ifConsumed == "1") { //取出来的是 1 表示已经是消费过的用户
                        orderInfo.if_first_order = "0"
                    } else {
                        orderInfo.if_first_order = "1"
                    }
                }
            }

            orderInfoList.toIterator
        }
//        orderInfoWithFlagDstream.print(1000)


        // 问题：
        //同一批次 同一个用户两次下单 如何解决 只保证第一笔订单为首单 其他订单不能为首单
        //矫正
        // 1  想办法让相同user_id的订单在一个分区中， 这样只要处理 mapPartition中的list就行了
        //--》 上游写入kafka 时 用userId 当分区键

        //2  groupbykey  按照某一个键值进行分组
        //   每组  取第一笔订单设为首单    非第一笔 设为 非首单   ，前提是：已经被全部设为首单
         val orderInfoGroupByUserIdDstream: DStream[(Long, Iterable[OrderInfo])] = orderInfoWithFlagDstream.map(orderInfo=>(orderInfo.user_id,orderInfo)).groupByKey()

        val orderInfoRealWithFirstFlagDstream: DStream[OrderInfo] = orderInfoGroupByUserIdDstream.flatMap {
            case (userId, orderInfoItr) =>
                val orderInfoList: List[OrderInfo] = orderInfoItr.toList
                if (orderInfoList != null && orderInfoList.size > 0) {
                    //从分组中任意取出一个用户，因为分组后，每个组的用户id一样
                    val orderInfoAny: OrderInfo = orderInfoList(0)
                    //需要修正的两个条件 1 、一个批次内做了2笔以上订单  2 、其中有首单  满足这两个条件就需要修正
                    if (orderInfoList.size >= 2 && orderInfoAny.if_first_order == "1") {
                        //根据下单时间进行排序
                        val sortedList: List[OrderInfo] = orderInfoList.sortWith { (orderInfoLeft, orderInfoRight) =>
                            orderInfoLeft.create_time < orderInfoRight.create_time
                        }
                        for (i <- 1 until sortedList.size) {
                            sortedList(i).if_first_order = "0"
                        }
                        sortedList
                    } else {
                        orderInfoList
                    }
                } else {
                    orderInfoList
                }
        }

//        orderInfoRealWithFirstFlagDstream.print(1000)



        //////////////////////////关联省份维度数据////////////////

        val orderInfoFinalWihtProvince: DStream[OrderInfo] = orderInfoRealWithFirstFlagDstream.transform { rdd =>
            val sql = "select ID,NAME,AREA_CODE,ISO_3166_2 from GMALL0213_PROVINCE_INFO"
            val provinceList: List[JSONObject] = PhoenixUtil.queryList(sql)
            val provinceMap: Map[lang.Long, JSONObject] = provinceList.map(jsonObj => (jsonObj.getLong("ID"), jsonObj)).toMap
            val provinceMapBC: Broadcast[Map[lang.Long, JSONObject]] = ssc.sparkContext.broadcast(provinceMap)

            val rddWithProvince: RDD[OrderInfo] = rdd.map { orderInfo =>
                val provinceInfoMap: Map[lang.Long, JSONObject] = provinceMapBC.value
                val provinceJsonObj: JSONObject = provinceInfoMap.getOrElse(orderInfo.province_id, null)
                if (provinceJsonObj != null) {
                    orderInfo.province_name = provinceJsonObj.getString("NAME")
                    orderInfo.province_area_code = provinceJsonObj.getString("AREA_CODE")
                    orderInfo.province_3166_2_code = provinceJsonObj.getString("ISO_3166_2")
                }
                orderInfo

            }
            rddWithProvince
        }
//        orderInfoFinalWihtProvince.print(1000)


        //////////////////////////关联用户维度数据////////////////
        val orderInfoFinalWithUserInfo: DStream[OrderInfo] = orderInfoFinalWihtProvince.mapPartitions { orderInfoItr =>
            val orderInfoList: List[OrderInfo] = orderInfoItr.toList
            if(orderInfoList !=null && orderInfoList.size>0){
                val userIdList: List[Long] = orderInfoList.map(orderInfo=>orderInfo.user_id)
                val sql = "select ID,USER_AGE_GROUP,USER_GENDER from GMALL0213_USER_INFO where ID in ('" + userIdList.mkString("','") + "')"
                val userInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
                val userInfoMap: Map[lang.Long, JSONObject] = userInfoList.map(jsonObj => (jsonObj.getLong("ID"), jsonObj)).toMap

                for (orderInfo <- orderInfoList ) {
                    val userIdJsonObj: JSONObject = userInfoMap.getOrElse(orderInfo.user_id,null)
                    if(userIdJsonObj !=null){
                        orderInfo.user_age_group = userIdJsonObj.getString("USER_AGE_GROUP")
                        orderInfo.user_gender = userIdJsonObj.getString("USER_GENDER")
                    }
                }
            }
            orderInfoList.toIterator
        }
        orderInfoFinalWithUserInfo.print(1000)


        //写入操作
        // 1  更新  用户状态
        // 2  存储olap  用户分析    可选
        // 3  推kafka 进入下一层处理   可选
        // 4 保存偏移量
        orderInfoFinalWithUserInfo.foreachRDD{
            rdd=>
              //缓存，不会重新在执行前面的过程
              rdd.cache()
                //TODO 1  更新  用户状态
              val userStateRDD = rdd.map(orderInfo=>UserState(orderInfo.user_id.toString,"1"))
                //spark phoenix的整合工具
                import org.apache.phoenix.spark._
                userStateRDD.saveToPhoenix("USER_STATE0213",
                    Seq("USER_ID","IF_CONSUMED"),
                    new Configuration,
                    Some("hadoop102,hadoop103,hadoop104:2181"))

                //TODO 2  存储olap  用户分析
                rdd.foreachPartition{orderInfoItr=>
                    val orderInfoList: List[OrderInfo] = orderInfoItr.toList
                    //将数据结构进行转换，为了幂等性，将订单id设置为主键
                    val orderInfoMapList: List[(OrderInfo, String)] = orderInfoList.map(orderInfo=>(orderInfo,orderInfo.id.toString))
                    val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())

//                    MyEsUtil.bulkSave(orderInfoMapList,"gmall0213_order_info_"+dateStr)
                 //TODO 3  推kafka 进入下一层处理
                    //遍历每条数据，写入kafka
                    for ((orderInfo,id)<- orderInfoMapList ) {
                        MyKafkaSink.send("DWD_ORDER_INFO",id,JSON.toJSONString(orderInfo,new SerializeConfig(true)))
                    }

                }
                //TODO 4 保存偏移量
                OffsetManager.saveOffset(topic,groupId,offsetRanges)


        }


        ssc.start()
        ssc.awaitTermination()

    }

}
