package com.atguigu.gmall0213.realtime.ads

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0213.realtime.bean.OrderWide
import com.atguigu.gmall0213.realtime.util.{MykafkaUtil, OffsetManager, OffsetManagerM}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

import scala.collection.mutable.ListBuffer

/**
 * @author lcy
 * @create 2020-08-01 16:13
 */
object TrademarkStatApp {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("ads_trademark_stat_app")
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val topic = "DWS_ORDER_WIDE"
        val groupId = "ads_trademark_stat_group"

        //从Mysql中读取偏移量
        val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerM.getOffset(topic,groupId)
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

        //提取数据
        val orderWideDstream: DStream[OrderWide] = inputGetOffsetDStream.map { record =>
            val json: String = record.value()
            val orderWide: OrderWide = JSON.parseObject(json, classOf[OrderWide])
            orderWide
        }

        //聚合
        val trademarkAmount: DStream[(String, Double)] = orderWideDstream.map{orderWide=>(orderWide.tm_id+"_" + orderWide.tm_name,orderWide.final_detail_amount)}
        val trademarkSumDstream: DStream[(String, Double)] = trademarkAmount.reduceByKey(_+_)


        trademarkSumDstream.print(1000)

        trademarkSumDstream.foreachRDD{rdd=>
            //把executor的数据提取到driver中
            val tmSumArr: Array[(String, Double)] = rdd.collect()
            if (tmSumArr.size > 0) {
                DBs.setup()
                DB.localTx { implicit session =>
                    //写入计算结果
                    val formator = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                    val dateTime = formator.format(new Date())
                    val dt: String = dateTime.split(" ")(0)
                    val batchParamsList: ListBuffer[Seq[Any]] = ListBuffer[Seq[Any]]()
                    for ((tm, amount) <- tmSumArr) {
                        val tmId: String = tm.split("_")(0)
                        val tmName: String = tm.split("_")(1)
                        val tmAmount = Math.round(amount * 100D) / 100D
                        batchParamsList.append(Seq(dt,dateTime,tmId,tmName,tmAmount))
                    }
                    //数据集合作为多个可变参数 的方法 的参数的时候 要加:_*
                    SQL("insert into trademark_detail_amount_stat(stat_date,stat_time,tm_id,tm_name,amount) values(?,?,?,?,?)").batch(batchParamsList.toSeq:_*).apply()

                    //写入偏移量
                    for (offsetRange <- offsetRanges ) {
                        val partitionId: Int = offsetRange.partition
                        val untilOffset: Long = offsetRange.untilOffset
                        SQL("replace into offset_0213  values(?,?,?,?)").bind(groupId, topic, partitionId, untilOffset).update().apply()
                    }
                }
            }
        }



        ssc.start()
        ssc.awaitTermination()
    }

}
