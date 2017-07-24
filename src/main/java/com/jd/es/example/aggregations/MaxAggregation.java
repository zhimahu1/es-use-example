package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;

/**
 * Title: MaxAggregation
 * Description: MaxAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class MaxAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.MaxAggregation.class);

    public static void main(String[] args) {
        maxAggregations(Tool.INDEX_NAME, Tool.TYPE_NAME, "height");//求height最大值
        maxAggregations(Tool.INDEX_NAME, Tool.TYPE_NAME, "age");//求age最大值
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
          "size": 0,
          "aggs": {
            "max_age": {
                  "max": {
                    "field": "age"
                  }
                }
          }
        }
     */
    private static void maxAggregations(String indexName, String typeName, String field) {

        MetricsAggregationBuilder aggregation = AggregationBuilders.max("agg").field(field);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        Max agg = sResponse.getAggregations().get("agg");
        double value = agg.getValue();
        logger.info(field + "最大值：" + value);
    }

}
