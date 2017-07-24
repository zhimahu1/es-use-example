package com.jd.es.example.aggregations;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

/**
 * Title: TermsAggregation2  组合terms和sum. 按照性别分类，统计每类的总的年龄
 * Description: TermsAggregation2
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/25
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class TermsAggregation2 {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.TermsAggregation2.class);


    public static void main(String[] args) {
        testTermsAggregation2(Tool.INDEX_NAME, Tool.TYPE_NAME, "gg", "gender");
    }


    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "size": 0,
              "aggs": {
                "by_gender": {
                  "terms": {
                    "field": "gender"
                  },
                  "aggs": {
                    "sum_age": {
                      "sum": {
                        "field": "age"
                      }
                    }
                  }
                }
              }
            }
     */
    private static void testTermsAggregation2(String indexName, String typeName,String termName, String field) {

        AggregationBuilder aggregation = AggregationBuilders
                .terms(termName)
                .field(field)
                .subAggregation(AggregationBuilders.sum("sumAge").field("age"));

        SearchRequestBuilder searchRequestBuilder = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setSize(0).addAggregation(aggregation);

        logger.info("请求：" + searchRequestBuilder);

        SearchResponse sResponse = searchRequestBuilder.get();

        Terms genders = sResponse.getAggregations().get(termName);
        // For each entry
        for (Terms.Bucket entry : genders.getBuckets()) {
            logger.info(entry.getKey() + " 文档数：" + entry.getDocCount());

            Aggregation agg = entry.getAggregations().get("sumAge");
            logger.info("需要的值:" + agg.getProperty("value"));
        }

        logger.info(sResponse);
    }
}