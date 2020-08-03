package com.atguigu.gmall0213.realtime.util

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition

/**
 * @author lcy
 * @create 2020-08-01 12:20
 */
object OffsetManagerM {

    def getOffset(topic:String,consumerGroupId:String):Map[TopicPartition,Long] ={

        //从mysql中获取偏移量
        val sql = "select group_id,topic,topic_offset,partition_id from offset_0213 where group_id = '" + consumerGroupId +"'and topic ='" +topic+"'"
        val offsetList: List[JSONObject] = MySqlUtil.queryList(sql)
        offsetList.map{offsetObj=>
            (new TopicPartition(topic,offsetObj.getIntValue("partition_id")),offsetObj.getLongValue("topic_offset"))
        }.toMap
    }

}
