package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;

/**
 * Title: PercentileAggregation  近似算法，有误差
 * Description: PercentileAggregation
 *       Percentiles show the point at which a certain percentage of observed values occur.
 *       For example, the 95th percentile is the value that is greater than 95% of the data.
 *       Percentiles are often used to find outliers 通常用来找出异常值
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class PercentileAggregation {
    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.PercentileAggregation.class);

    public static void main(String[] args) {
        percentileAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "height");
        percentileAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "age");
    }


    /**
     * 0% 最小值
     * 100% 最大值
     *
     * 如果10%为1,30%为3，1,3之间没有数，那20%为2
     */
    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "size": 0,
              "aggs": {
                "stats_age": {
                  "percentiles": {
                    "field": "age"
                  }
                }
              }
            }
     */
    private static void percentileAggregation(String indexName, String typeName, String field) {

        //By default, the percentiles metric will return an array of predefined percentiles: [1, 5, 25, 50, 75, 95, 99]

        //MetricsAggregationBuilder aggregation = AggregationBuilders.percentiles("agg").field(field);
        //You can provide your own percentiles instead of using defaults:
        MetricsAggregationBuilder aggregation = AggregationBuilders.percentiles("agg").field(field).percentiles(0.0,1.0, 7.125, 12.5, 14.25,20.0,28.5,35.625,42.75, 75.0, 95.0, 99.0,100,0);

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        Percentiles agg = sResponse.getAggregations().get("agg");
        // For each entry
        for (Percentile entry : agg) {
            double percent = entry.getPercent();    // Percent
            double value = entry.getValue();        // Value

            logger.info("percent:"+ percent +" value:"+value);
        }
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "size": 0,
              "aggs": {
                "country_agg": {
                  "terms": {
                    "field": "country"
                  },
                  "aggs": {
                    "height_perc": {
                      "percentiles": {
                        "field": "height",
                        "percents": [50,95,99]
                      }
                    },
                    "age_avg": {
                      "avg": {
                        "field": "age"
                      }
                    }
                  }
                }
              }
            }
     */


}
