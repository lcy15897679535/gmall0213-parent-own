package com.atguigu.gmall0213.realtime.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * @author lcy
 * @create 2020-07-24 20:11
 */
object MyKafkaSink {

    private val properties: Properties = PropertiesUtil.load("config.properties")
    val broker_list = properties.getProperty("kafka.broker.list")
    var kafkaProducer: KafkaProducer[String, String] = null

    // kafka生产者配置
    def createKafkaProducer: KafkaProducer[String, String] = {
        val properties = new Properties
        properties.put("bootstrap.servers", broker_list)
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        properties.put("enable.idempotence",(true: java.lang.Boolean))

        var producer: KafkaProducer[String, String] = null
        try

            producer = new KafkaProducer[String, String](properties)
        catch {
            case e: Exception =>
                e.printStackTrace()
        }
        producer
    }

    def send(topic: String, msg: String): Unit = {
        if (kafkaProducer == null) kafkaProducer = createKafkaProducer
        kafkaProducer.send(new ProducerRecord[String, String](topic, msg))

    }

    def send(topic: String,key:String, msg: String): Unit = {
        if (kafkaProducer == null) kafkaProducer = createKafkaProducer
        kafkaProducer.send(new ProducerRecord[String, String](topic,key, msg))

    }

}
