package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

/**
 * Title: SumAggregation
 * Description: SumAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class SumAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.SumAggregation.class);

    public static void main(String[] args) {
        SumAggregations(Tool.INDEX_NAME, Tool.TYPE_NAME, "height");
        SumAggregations(Tool.INDEX_NAME, Tool.TYPE_NAME, "age");

        SumAggregations2(Tool.INDEX_NAME, Tool.TYPE_NAME);
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
          "size": 0,
          "aggs": {
            "sum_age": {
                  "sum": {
                    "field": "age"
                  }
                }
          }
        }
     */
    private static void SumAggregations(String indexName, String typeName, String field) {

        MetricsAggregationBuilder aggregation = AggregationBuilders.sum("agg").field(field);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        Sum agg = sResponse.getAggregations().get("agg");
        double value = agg.getValue();
        logger.info(field + " ：" + value);
    }


    /*  HTTP 接口
    URL:agg_index/agg_type/_search
    TYPE：POST
    BODY:
        {
          "size": 0,
          "aggs": {
            "term_by_country": {
              "terms": {
                "field": "country"
              },
              "aggs": {
                "sumAge": {
                  "sum": {
                    "field": "age"
                  }
                }
              }
            }
          }
        }
 */
    private static void SumAggregations2(String indexName, String typeName) {

        AggregationBuilder aggregation = AggregationBuilders
                .terms("term_by_country")
                .field("country")
                .subAggregation(AggregationBuilders.sum("sumAge").field("age"));

        SearchRequestBuilder searchRequestBuilder = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setSize(0).addAggregation(aggregation);
        logger.info("请求体：" + searchRequestBuilder);

        SearchResponse sResponse = searchRequestBuilder.get();
        logger.info("返回聚合结果：" + sResponse);

        Terms genders = sResponse.getAggregations().get("term_by_country");
        // For each entry
        for (Terms.Bucket entry : genders.getBuckets()) {
            String clusterName = (String) entry.getKey();
            long docNum = Math.round((double) entry.getAggregations().get("sumAge").getProperty("value"));
            logger.info(clusterName + " " + docNum);
        }
    }
}