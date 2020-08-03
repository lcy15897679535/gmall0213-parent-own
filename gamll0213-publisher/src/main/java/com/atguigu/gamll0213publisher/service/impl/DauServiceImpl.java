package com.atguigu.gamll0213publisher.service.impl;

import com.atguigu.gamll0213publisher.service.DauService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author lcy
 * @create 2020-07-23 19:20
 */
@Service
public class DauServiceImpl implements DauService {


    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {

        //用索引的别名来查询
        String indexName = "gmall_dau_info0213_"+date.replace("-","")+"_query";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询所有的数据
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();

        try {
            SearchResult searchResult = jestClient.execute(search);
            //返回查询的总数
            return  searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询异常");
        }

    }

    @Override
    public Map getDauHour(String date) {
        //用索引的别名来查询
        String indexName = "gmall_dau_info0213_"+date.replace("-","")+"_query";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //根据hr分组，查询聚合值
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_hour").field("hr").size(24);
        //执行聚合命令： aggregation 可能有多个聚合命令使用 ：aggregations
        searchSourceBuilder.aggregation(termsBuilder);
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            Map<Object, Object> resultMap = new HashMap<>();
            if(searchResult.getAggregations().getTermsAggregation("groupby_hour")!=null){
                //searchResult返回结果 是list集合 从0时 到24时
                List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_hour").getBuckets();
                //循环遍历，将数据放入map 中
                for (TermsAggregation.Entry bucket : buckets) {
                   resultMap.put(bucket.getKey(),bucket.getCount());
                }
                return resultMap;
            }else {
                return new HashMap();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询异常");
        }

    }
}
