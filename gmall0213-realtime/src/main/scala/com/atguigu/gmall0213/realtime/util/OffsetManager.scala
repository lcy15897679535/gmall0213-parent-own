package com.atguigu.gmall0213.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.{Jedis, JedisCluster}

import scala.collection.JavaConversions._
/**
 * @author lcy
 * @create 2020-07-19 10:50
 */
object OffsetManager {

    def getOffset(topic:String,consumerGroupId:String):Map[TopicPartition,Long] ={
        //TODO  redis  type? hash   key  ? 主题1：消费者组1  field ?  分区 value ?偏移量
        val jedis: JedisCluster = RedisUtil.getJedisCluster()
        val offsetKey = topic + ":" + consumerGroupId
        //TODO 取出field 和 value
        val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)

        //TODO 判断取出来的field 和 value 是否为空
        if(offsetMap !=null && offsetMap.size()>0){
            val offsetList: List[(String, String)] = offsetMap.toList

            //TODO 将field和value组成的集合 转化为 kafka需要的类型 （topicPartition，offset）
            val offsetListForKafka: List[(TopicPartition, Long)] = offsetList.map {
                case (partition, offset) =>
                    val topicPartition: TopicPartition = new TopicPartition(topic, partition.toInt)
                    println("加载偏移量：分区："+partition+"==>"+offset)
                    (topicPartition, offset.toLong)
            }
            val offsetMapForKafka: Map[TopicPartition, Long] = offsetListForKafka.toMap
            offsetMapForKafka
        }else{
            null
        }

    }

    def saveOffset(topic:String,consumerGroupId:String,offsetRanges:Array[OffsetRange]): Unit = {
        val offsetKey = topic + ":" + consumerGroupId
        //TODO 用来存储多个分区的偏移量
        val offsetMap: util.HashMap[String, String] = new util.HashMap[String,String]()
        for (offsetRange <- offsetRanges ) {
            val partition: String = offsetRange.partition.toString
            val untilOffset: String = offsetRange.untilOffset.toString
//            println("写入偏移量：分区："+partition+"==>"+untilOffset)
            offsetMap.put(partition,untilOffset)
        }

        val jedis: JedisCluster = RedisUtil.getJedisCluster()
        jedis.hmset(offsetKey,offsetMap)


    }

}
