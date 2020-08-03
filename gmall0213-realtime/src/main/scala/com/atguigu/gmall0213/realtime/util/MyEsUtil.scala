package com.atguigu.gmall0213.realtime.util

import java.util
import java.util.Properties

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder

/**
 * @author lcy
 * @create 2020-07-21 15:40
 */
object MyEsUtil {

    private var factory: JestClientFactory = null

    def getJestClient():JestClient ={

        if(factory!=null){

            factory.getObject
        }else{
            build()
            factory.getObject
        }
    }

    def build(): Unit ={
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
     * 写操作,单条写入
     */
    def saveToEs(): Unit ={
        val jestClient: JestClient = getJestClient()

//        val index: Index = new Index.Builder(Movie("1003","局中人")).index("movie_test200722").`type`("_doc").id("1003").build()
        //写操作
//        jestClient.execute(index)

        jestClient.close()
    }


    /**
     * 批量写入
     * @param list
     * @param IndexName
     */
    def bulkSave(list:List[(Any,String)],IndexName:String)={
        val jestClient: JestClient = getJestClient()
        val builder = new Bulk.Builder
        builder.defaultIndex(IndexName).defaultType("_doc")

        for ((doc, id) <- list ) {
            val index: Index = new Index.Builder(doc).id(id).build()
            builder.addAction(index)
        }

        val bulkAction: Bulk = builder.build()
        val items: util.List[BulkResult#BulkResultItem] = jestClient.execute(bulkAction).getItems
        println("已保存："+ items.size())

        jestClient.close()
    }





    /**
     * 查询
     */
    def queryFromEs(): Unit ={
        val jestClient: JestClient = getJestClient()
        //方式一
        val query = "{\n  \"query\": {\n    \"match\": {\n      \"name\": \"红海\"\n    }\n  },\n  \"sort\": [\n    {\n      \"doubanScore\": {\n        \"order\": \"desc\"\n      }\n    }\n  ],\n  \"from\": 0,\n  \"size\": 20\n}"
        val search: Search = new Search.Builder(query).addIndex("movie_chn").addType("movie").build()

        val result: SearchResult = jestClient.execute(search)
        val resultList: util.List[SearchResult#Hit[util.Map[String,Object], Void]] = result.getHits(classOf[util.Map[String,Object]])

        import collection.JavaConversions._
        for (hit <- resultList ) {

            val source: util.Map[String, Object] = hit.source
//            println(source)
        }



        //方式二
        val searchSourceBuilder = new SearchSourceBuilder
        searchSourceBuilder.query(new MatchQueryBuilder("name","red"))
        searchSourceBuilder.sort("doubanScore",SortOrder.ASC)

        val query2: String = searchSourceBuilder.toString
        println(query2)
        val search1: Search = new Search.Builder(query2).addIndex("movie_index0213").addType("movie").build()
        val result1: SearchResult = jestClient.execute(search1)

        val resultList1: util.List[SearchResult#Hit[util.Map[String,Object], Void]] = result1.getHits(classOf[util.Map[String,Object]])

        import collection.JavaConversions._
        for (hit <- resultList1 ) {

            val source: util.Map[String, Object] = hit.source
                        println(source)
        }

        jestClient.close()
    }


    def main(args: Array[String]): Unit = {
        saveToEs()
//        queryFromEs()
    }

}

//case class Movie(id:String,movie_name:String)
