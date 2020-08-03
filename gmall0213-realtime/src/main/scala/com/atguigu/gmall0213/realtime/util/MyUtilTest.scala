package com.atguigu.gmall0213.realtime.util

import java.util
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Index, Search, SearchResult}
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.mutable

/**
 * @author lcy
 * @create 2020-07-21 22:53
 */
object MyUtilTest {

    private var factory :JestClientFactory = null

    def getJestClient()={
        if(factory != null){
            factory.getObject
        }else{
            bulid()
            factory.getObject
        }
    }

    def bulid(): Unit ={
        factory = new JestClientFactory
        val properties: Properties = PropertiesUtil.load("config.properties")
        val serverUri: String = properties.getProperty("serverUri")
        factory.setHttpClientConfig(new HttpClientConfig.Builder(serverUri)
          .multiThreaded(true)
          .maxTotalConnection(20)
          .connTimeout(10000).readTimeout(1000)
          .build()
        )
    }

    /**
     * 写操作
     */
    def saveToEs(jsonObject:JSONObject,movie_index: String,movie_type:String): Unit ={
        val jestClient: JestClient = getJestClient()

        val map = new util.HashMap[String,String]()
        map.put("id","1007")
        map.put("movie_name","港囧")
//        println(map)

        val index = new Index.Builder(jsonObject).index(movie_index).`type`(movie_type).build()

        jestClient.execute(index)


        jestClient.close()
    }

    /**
     * 查询
     */
    def queryFromEs(): Unit ={
        val jestClient: JestClient = getJestClient()
        val searchSourceBuilder = new SearchSourceBuilder

        searchSourceBuilder.query(new MatchQueryBuilder("movie_name","速度"))
        searchSourceBuilder.from(0)
        searchSourceBuilder.size(20)
        val query: String = searchSourceBuilder.toString
        println(query)

        val search: Search = new Search.Builder(query).addIndex("movie_test200722").addType("_doc").build()

        val result: SearchResult = jestClient.execute(search)

        val resultList: util.List[SearchResult#Hit[util.Map[String, Object], Void]] = result.getHits(classOf[util.Map[String,Object]])

        import collection.JavaConversions._
        for (hit <- resultList ) {
            val source: util.Map[String, Object] = hit.source
            println(source)

        }


        jestClient.close()
    }


    def main(args: Array[String]): Unit = {
        val id = "1008"
        val movie_name = "我是谁"
        val json =
            s"""
              |{"id":"$id","movie_name":"$movie_name"}
              """.stripMargin
        val jsonObject: JSONObject = JSON.parseObject(json)

        saveToEs(jsonObject,"movie_test200722","_doc")

//        queryFromEs()
    }

}
