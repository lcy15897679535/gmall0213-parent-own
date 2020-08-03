package com.atguigu.gmall0213.realtime.ods



import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall0213.realtime.util.{MyKafkaSink, MykafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lcy
 * @create 2020-07-24 19:39
 */
object BaseDbCanalApp {

    def main(args: Array[String]): Unit = {


        //  偏移量管理？    精确一次消费？ kafka作为最后存储端 无法保证幂等性 只能做“至少一次消费”
        //  手动后置偏移量必须保证  防止宕机丢失数据
        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("base_db_canal_app")
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val topic = "GMALL0213_DB_C"
        val groupId = "base_db_canal_group"

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


        //TODO 业务操作，提取数据，分到不同topic中
        val jsonObjDStream: DStream[JSONObject] = inputGetOffsetDStream.map {
            record =>
                val jsonString: String = record.value()
                val jsonObj: JSONObject = JSON.parseObject(jsonString)
                jsonObj
        }
        jsonObjDStream.foreachRDD{
            rdd=>
                rdd.foreach{
                    jsonObj=>
                    //解析json
                        val tableName = jsonObj.getString("table")
                        val optType = jsonObj.getString("type")
                        val topicName = "ODS_" + tableName.toUpperCase
                        val dataArr: JSONArray = jsonObj.getJSONArray("data")

                        if((tableName.equals("order_info") && optType.equals("INSERT"))||
                          (tableName.equals("order_detail") && optType.equals("INSERT"))
                        ) {
                            for(i <- 0 until dataArr.size()){

                                val json: String = dataArr.getString(i)
                                //发送到kafka主题
                                MyKafkaSink.send(topicName,json)
                            }
                        }
                }
                //driver 提交偏移量
                OffsetManager.saveOffset(topic,groupId,offsetRanges)
        }


        ssc.start()
        ssc.awaitTermination()
    }

}
