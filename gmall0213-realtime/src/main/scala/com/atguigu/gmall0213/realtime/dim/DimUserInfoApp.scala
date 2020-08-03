package com.atguigu.gmall0213.realtime.dim

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, GregorianCalendar}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0213.realtime.bean.UserInfo
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
 * @create 2020-07-26 13:48
 */
object DimUserInfoApp {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dim_user_info_app")
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val topic = "ODS_USER_INFO"
        val groupId = "dim_user_info_group"

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

        val userInfoDstream: DStream[UserInfo] = inputGetOffsetDStream.map { record =>
            val json: String = record.value()
            val jsonObj: JSONObject = JSON.parseObject(json)
            val gender: String = jsonObj.getString("gender")
            //判断性别
            var user_gender: String = null
            if (gender.equals("F")) {
                user_gender = "男"
            } else {
                user_gender = "女"
            }
            //求出年龄
            val birthday: String = jsonObj.getString("birthday")
            val age: Long = getAgeGroup(birthday)
            var user_age_group: String = null
            if(age<=20){
                user_age_group = "20岁以下"
            }else if(age>20 && age <=30){
                user_age_group = "21岁~30岁之间"
            }else{
                user_age_group = "31岁以上"
            }


            val userInfo = UserInfo(
                jsonObj.getString("id"),
                user_age_group,
                user_gender
            )
            userInfo

        }

        userInfoDstream.print(1000)
        //4  1 写入phoenix 2 提交偏移量
        userInfoDstream.foreachRDD{rdd=>
            import org.apache.phoenix.spark._
            rdd.saveToPhoenix("GMALL0213_USER_INFO",
                Seq("ID","USER_AGE_GROUP","USER_GENDER"),
                new Configuration,
                Some("hadoop102,hadoop103,hadoop104:2181"))

            OffsetManager.saveOffset(topic,groupId,offsetRanges)
        }

        ssc.start()
        ssc.awaitTermination()

    }

    //计算年龄
    def getAgeGroup(birthday:String)={

        val date = new Date()
        val dt: String = date.toString.split(" ")(5)
        val date1 = new SimpleDateFormat("yyyy").parse(dt)
        val todayTime: Long = date1.getTime
        val birDate: Date = new SimpleDateFormat("yyyy").parse(birthday)
        val birTime: Long = birDate.getTime
        val time: Long = (todayTime - birTime)
        val ts: Long = time /(365*60*60*24)
        val age: Long = ts/1000
        age




    }


}
