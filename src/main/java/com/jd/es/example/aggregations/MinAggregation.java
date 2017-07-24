package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;

/**
 * Title: MinAggregation
 * Description: MinAggregation 查找最小值
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class MinAggregation {

    public static void main(String[] args) {
        minAggregations(Tool.INDEX_NAME, Tool.TYPE_NAME, "height");//求height最小值
        minAggregations(Tool.INDEX_NAME, Tool.TYPE_NAME, "age");//求age最小值
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
          "size": 0,
          "aggs": {
            "min_age": {
                  "min": {
                    "field": "age"
                  }
                }
          }
        }
     */
    private static void minAggregations(String indexName, String typeName, String field) {

        MetricsAggregationBuilder aggregation = AggregationBuilders.min("agg").field(field);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        Min agg = sResponse.getAggregations().get("agg");
        double value = agg.getValue();
        System.out.println(field + "最小值：" + value);
    }
}
