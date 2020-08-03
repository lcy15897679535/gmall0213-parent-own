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
 * @create 2020-08-02 15:26
 */
object UserGenderStatApp {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("ads_user_gender_stat_app")
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val topic = "DWS_ORDER_WIDE"
        val groupId = "ads_user_gender_stat_group"

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


        //聚合数据
        val userGenderDstream: DStream[(String, Double)] = orderWideDstream.map(orderWide=>(orderWide.user_gender,orderWide.final_detail_amount))
        val userGenderSumDstream: DStream[(String, Double)] = userGenderDstream.reduceByKey(_+_)
        val userGenderRoundDstream: DStream[(String, Double)] = userGenderSumDstream.map {
            case (userGender, amount) =>
                val roundAmount = Math.round(amount * 100D) / 100D
                (userGender, roundAmount)
        }
        userGenderRoundDstream.print(1000)

        userGenderRoundDstream.foreachRDD { rdd =>
            //将数据收集到driver端
            val userGenderArr = rdd.collect()
            if (userGenderArr.size > 0) {
                //将数据写入到mysql中
                DBs.setup()
                DB.localTx { implicit session =>
                    val formator: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                    val dateTime: String = formator.format(new Date())
                    val dt = dateTime.split(" ")(0)
                    val batchParamsList: ListBuffer[Seq[Any]] = ListBuffer[Seq[Any]]()
                    for ((useGender, amount) <- userGenderArr) {
                        batchParamsList.append(Seq(dt,dateTime,useGender,amount))
                    }
                    SQL("insert into user_gender_amount_stat (stat_date,stat_time,user_gender,amount) values(?,?,?,?)").batch(batchParamsList.toSeq:_*).apply()

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
