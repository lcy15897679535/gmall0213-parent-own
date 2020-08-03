package com.atguigu.gmall0213.realtime.dim

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0213.realtime.bean.{Category3Info, SpuInfo}
import com.atguigu.gmall0213.realtime.util.{MykafkaUtil, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
 * @author lcy
 * @create 2020-07-27 21:32
 */
object DimBaseCategory3App {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dim_base_category3_app")
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val topic = "ODS_BASE_CATEGORY3"
        val groupId = "dim_base_category3_group"

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


        val category3InfoDstream: DStream[Category3Info] = inputGetOffsetDStream.map { record =>
            val json: String = record.value()
            val category3Info: Category3Info = JSON.parseObject(json, classOf[Category3Info])
            category3Info
        }

        category3InfoDstream.print(20)
        category3InfoDstream.foreachRDD{rdd=>

            import org.apache.phoenix.spark._
            rdd.saveToPhoenix("GMALL0213_CATEGORY3_INFO",
                Seq("ID","NAME"),
                new Configuration,
                Some("hadoop102,hadoop103,hadoop104:2181")
            )
            OffsetManager.saveOffset(topic,groupId,offsetRanges)


        }


        ssc.start()
        ssc.awaitTermination()
    }

}
