package com.atguigu.gmall0213.realtime.dim

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0213.realtime.bean.ProvinceInfo
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
 * @create 2020-07-26 13:14
 */
object DimBaseProvinceApp {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dim_base_province_app")
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val topic = "ODS_BASE_PROVINCE"
        val groupId = "dim_base_province_group"

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
        
        
        //业务操作 将数据结构进行转换成样例类，方便后续操作
        val provinceInfoDstream: DStream[ProvinceInfo] = inputGetOffsetDStream.map {
            record =>
                val json: String = record.value()
                val provinceInfo: ProvinceInfo = JSON.parseObject(json, classOf[ProvinceInfo])
                provinceInfo
        }
        //4  1 写入phoenix 2 提交偏移量
        provinceInfoDstream.foreachRDD{
            rdd=>
                import org.apache.phoenix.spark._
                rdd.saveToPhoenix("GMALL0213_PROVINCE_INFO",
                    Seq("ID","NAME","AREA_CODE","ISO_CODE","ISO_3166_2"),
                    new Configuration,
                    Some("hadoop102,hadoop103,hadoop104:2181"))
                OffsetManager.saveOffset(topic,groupId,offsetRanges)
        }

        ssc.start()
        ssc.awaitTermination()
    }

}
