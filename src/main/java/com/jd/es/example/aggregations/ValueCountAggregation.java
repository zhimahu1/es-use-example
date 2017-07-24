package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;

/**
 * Title: ValueCountAggregation
 * Description: ValueCountAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class ValueCountAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.ValueCountAggregation.class);

    public static void main(String[] args) {
        valueCountAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "height");
        valueCountAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "age");
        valueCountAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "gender");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "size": 0,
              "aggs": {
                "aggs_name": {
                  "value_count": {
                    "field": "age"
                  }
                }
              }
            }
     */
    private static void valueCountAggregation(String indexName, String typeName, String field) {

        MetricsAggregationBuilder aggregation = AggregationBuilders.count("agg").field(field);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        ValueCount agg = sResponse.getAggregations().get("agg");
        double value = agg.getValue();
        logger.info(field + " ValueCount：" + value);
    }

}
