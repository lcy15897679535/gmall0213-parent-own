package com.atguigu.gmall0213.realtime.ads

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0213.realtime.bean.OrderWide
import com.atguigu.gmall0213.realtime.util.{MykafkaUtil, OffsetManagerM}
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
 * @create 2020-08-02 12:40
 */
object Category3StatApp {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("ads_category3_stat_app")
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val topic = "DWS_ORDER_WIDE"
        val groupId = "ads_category3_stat_group"

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
        val category3AmountDstream: DStream[(String, Double)] = orderWideDstream.map(orderWide=>(orderWide.category3_id+"_"+orderWide.category3_name,orderWide.final_detail_amount))

        val category3SumDstream: DStream[(String, Double)] = category3AmountDstream.reduceByKey(_+_)

        val category3RoundSumDstream: DStream[(String, Double)] = category3SumDstream.map {
            case (category3, amount) =>
                val roundAmount = Math.round(amount * 100D) / 100D
                (category3, roundAmount)
        }
        category3RoundSumDstream.print(1000)


        category3RoundSumDstream.foreachRDD{rdd=>
            //将数据收集到driver端
            val category3Arr= rdd.collect()
            if(category3Arr.size>0){
                //数据保存到mysql
                DBs.setup()
                DB.localTx{implicit session=>
                    val formator: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                    val dateTime: String = formator.format(new Date())
                    val dt = dateTime.split(" ")(0)
                    val batchParamsList: ListBuffer[Seq[Any]] = ListBuffer[Seq[Any]]()
                    for ((category3, amount) <- category3Arr) {
                        val category3Id = category3.split("_")(0)
                        val category3Name = category3.split("_")(1)
                        batchParamsList.append(Seq(dt,dateTime,category3Id,category3Name,amount))
                    }
                    SQL("insert into category3_amount_stat (stat_date,stat_time,category3_id,category3_name,amount) values(?,?,?,?,?)").batch(batchParamsList.toSeq:_*).apply()

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