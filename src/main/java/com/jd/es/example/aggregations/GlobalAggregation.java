package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.global.Global;

/**
 * Title: GlobalAggregation 全局聚合
 * Description: GlobalAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class GlobalAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.GlobalAggregation.class);

    public static void main(String[] args) {
        globalAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "height");
        globalAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "age");
    }


    /**
     注意下面的http接口中，有没有如下query部分，都不影响聚合的结果，只影响hits的结果
    "query": {
        "match": {
            "country": "China"
        }
    }
     */
    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "match": {
                  "country": "China"
                }
              },
              "aggs": {
                "all_products": {
                  "global": {},
                  "aggs": {
                    "avg_price": {
                      "avg": {
                        "field": "age"
                      }
                    }
                  }
                }
              }
            }
     */
    private static void globalAggregation(String indexName, String typeName, String field) {

        AbstractAggregationBuilder aggregation =
        AggregationBuilders
                .global("agg")
                .subAggregation(AggregationBuilders.terms("term_name").field(field));

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        Global agg = sResponse.getAggregations().get("agg");
        long docCount = agg.getDocCount(); // Doc count
        logger.info("docCount:"+docCount);
        logger.info(sResponse);
    }
}
