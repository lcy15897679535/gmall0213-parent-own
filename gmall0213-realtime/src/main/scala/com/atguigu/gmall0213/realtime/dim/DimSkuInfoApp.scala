package com.atguigu.gmall0213.realtime.dim

import java.lang

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0213.realtime.bean.SkuInfo
import com.atguigu.gmall0213.realtime.util.{MykafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

import scala.collection.mutable.ListBuffer

/**
 * @author lcy
 * @create 2020-07-27 21:36
 */
object DimSkuInfoApp {

    def main(args: Array[String]): Unit = {


        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dim_sku_info_app")
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val topic = "ODS_SKU_INFO"
        val groupId = "dim_sku_info_group"

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


        val skuInfoDstream: DStream[SkuInfo] = inputGetOffsetDStream.map { record =>
            val json: String = record.value()
            val skuInfo: SkuInfo = JSON.parseObject(json, classOf[SkuInfo])
            skuInfo
        }


        val skuInfoFinalDstream: DStream[SkuInfo] = skuInfoDstream.transform { rdd =>
            //spu_info
            val spuSql = "select ID,SPU_NAME from GMALL0213_SPU_INFO"
            val spuInfoList: List[JSONObject] = PhoenixUtil.queryList(spuSql)
            val spuInfoMap: Map[lang.Long, JSONObject] = spuInfoList.map(spuInfo => (spuInfo.getLong("ID"), spuInfo)).toMap


            //trademark_info
            val TmSql = "select TM_ID,TM_NAME from GMALL0213_TRADEMARK_INFO"
            val tmInfoList: List[JSONObject] = PhoenixUtil.queryList(TmSql)
            val tmInfoMap: Map[lang.Long, JSONObject] = tmInfoList.map(tmInfo => (tmInfo.getLong("TM_ID"), tmInfo)).toMap


            //category3_info
            val Category3Sql = "select ID,NAME from GMALL0213_CATEGORY3_INFO"
            val category3InfoList: List[JSONObject] = PhoenixUtil.queryList(Category3Sql)
            val category3InfoMap: Map[lang.Long, JSONObject] = category3InfoList.map(category3Info => (category3Info.getLong("ID"), category3Info)).toMap


            //将广播变量封装到一个list集合中
            val dimList = List[Map[lang.Long, JSONObject]](spuInfoMap, tmInfoMap, category3InfoMap)
            val dimBC: Broadcast[List[Map[lang.Long, JSONObject]]] = ssc.sparkContext.broadcast(dimList)

            val skuInfoRDD: RDD[SkuInfo] = rdd.mapPartitions { skuInfoItr =>
                //接收bc
                val dimList: List[Map[lang.Long, JSONObject]] = dimBC.value
                val spuMap: Map[lang.Long, JSONObject] = dimList(0)
                val tmMap: Map[lang.Long, JSONObject] = dimList(1)
                val category3Map: Map[lang.Long, JSONObject] = dimList(2)

                val skuInfoList: List[SkuInfo] = skuInfoItr.toList
                for (skuInfo <- skuInfoList) {
                    val spuJsonObj: JSONObject = spuMap.getOrElse(skuInfo.spu_id, null)
                    if (spuJsonObj != null) {
                        skuInfo.spu_name = spuJsonObj.getString("SPU_NAME")
                    }
                    val tmJsonObj: JSONObject = tmMap.getOrElse(skuInfo.tm_id, null)
                    if (tmJsonObj != null) {
                        skuInfo.tm_name = tmJsonObj.getString("TM_NAME")
                    }
                    val category3JsonObj: JSONObject = category3Map.getOrElse(skuInfo.category3_id, null)
                    if (category3JsonObj != null) {
                        skuInfo.category3_name = category3JsonObj.getString("NAME")
                    }
                }
                skuInfoList.toIterator
            }
            skuInfoRDD
        }

/*        //关联维度spu_info
        val skuInfoWithSpu: DStream[SkuInfo] = skuInfoDstream.transform { rdd =>
            val sql = "select ID,SPU_NAME from GMALL0213_SPU_INFO"
            val spuInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
            val spuInfoMap: Map[lang.Long, JSONObject] = spuInfoList.map(spuInfo => (spuInfo.getLong("ID"), spuInfo)).toMap
            val spuInfoBC: Broadcast[Map[lang.Long, JSONObject]] = ssc.sparkContext.broadcast(spuInfoMap)

            val rddWithSpu: RDD[SkuInfo] = rdd.map { skuInfo =>
                val spuInfoJsonObj: Map[lang.Long, JSONObject] = spuInfoBC.value
                val spuInfoObj: JSONObject = spuInfoJsonObj.getOrElse(skuInfo.spu_id, null)
                if (spuInfoObj != null) {
                    skuInfo.spu_name = spuInfoObj.getString("SPU_NAME")
                }
                skuInfo
            }
            rddWithSpu
        }
        skuInfoWithSpu.print(1000)
        //关联维度tm_info

        val skuInfoWithTMDstream: DStream[SkuInfo] = skuInfoWithSpu.transform { rdd =>
            val sql = "select TM_ID,TM_NAME from GMALL0213_TRADEMARK_INFO"
            val tmInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
            val tmInfoMap: Map[lang.Long, JSONObject] = tmInfoList.map(tmInfo => (tmInfo.getLong("TM_ID"), tmInfo)).toMap
            val tmInfoBC: Broadcast[Map[lang.Long, JSONObject]] = ssc.sparkContext.broadcast(tmInfoMap)

            val rddWithTm: RDD[SkuInfo] = rdd.map { skuInfo =>
                val tmInfoJsonObj: Map[lang.Long, JSONObject] = tmInfoBC.value
                val tmInfoObj: JSONObject = tmInfoJsonObj.getOrElse(skuInfo.tm_id, null)
                if (tmInfoObj != null) {
                    skuInfo.tm_name = tmInfoObj.getString("TM_NAME")
                }
                skuInfo
            }
            rddWithTm
        }


        //关联品类 category3
        val skuInfoFinalDstream: DStream[SkuInfo] = skuInfoWithTMDstream.transform { rdd =>
            val sql = "select ID,NAME from GMALL0213_CATEGORY3_INFO"
            val category3InfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
            val category3InfoMap: Map[lang.Long, JSONObject] = category3InfoList.map(category3Info => (category3Info.getLong("ID"), category3Info)).toMap
            val category3InfoMapBC: Broadcast[Map[lang.Long, JSONObject]] = ssc.sparkContext.broadcast(category3InfoMap)

            val rddWithCategory3: RDD[SkuInfo] = rdd.map { skuInfo =>
                val category3InfoJsonObj: Map[lang.Long, JSONObject] = category3InfoMapBC.value
                val category3InfoObj: JSONObject = category3InfoJsonObj.getOrElse(skuInfo.category3_id, null)
                if (category3InfoObj != null) {
                    skuInfo.category3_name = category3InfoObj.getString("NAME")
                }
                skuInfo
            }
            rddWithCategory3
        }
        skuInfoFinalDstream.print(1000)*/
        skuInfoFinalDstream.print(1000)


        skuInfoFinalDstream.foreachRDD{rdd=>

            import org.apache.phoenix.spark._
            rdd.saveToPhoenix("GMALL0213_SKU_INFO",
                Seq("ID","SPU_NAME","SPU_ID","CATEGORY3_ID","TM_ID","TM_NAME","CATEGORY3_NAME"),
                new Configuration,
                Some("hadoop102,hadoop103,hadoop104:2181")
            )
            OffsetManager.saveOffset(topic,groupId,offsetRanges)
        }

        ssc.start()
        ssc.awaitTermination()
    }

}
