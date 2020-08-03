package com.atguigu.gmall0213.realtime.util

import java.util
import java.util.Properties

import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPool, JedisPoolConfig}

import scala.collection.mutable

/**
 * @author lcy
 * @create 2020-07-17 22:30
 */
object RedisUtil {

    var jedisCluster:JedisCluster = null

    def getJedisCluster() ={
        if(jedisCluster == null){
            val config: Properties = PropertiesUtil.load("config.properties")
            val host: String = config.getProperty("redis.host")
            val port: String = config.getProperty("redis.port")
            val hostAndPorts = new util.HashSet[HostAndPort]()
            hostAndPorts.add(new HostAndPort(host,port.toInt))

            val jedisPoolConfig = new JedisPoolConfig()
            jedisPoolConfig.setMaxTotal(100)  //最大连接数
            jedisPoolConfig.setMaxIdle(20)   //最大空闲
            jedisPoolConfig.setMinIdle(20)     //最小空闲
            jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
            jedisPoolConfig.setMaxWaitMillis(500)//忙碌时等待时长 毫秒
            jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试


            jedisCluster = new JedisCluster(hostAndPorts,jedisPoolConfig)
            jedisCluster
        }else{
            jedisCluster
        }

    }

}
