package com.jd.es.example.aggregations;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;

/**
 * Title: ExtendedStatsAggregation
 * Description: ExtendedStatsAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class ExtendedStatsAggregation {
    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.ExtendedStatsAggregation.class);

    public static void main(String[] args) {
        extendedStatsAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "height");
        extendedStatsAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "age");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "size": 0,
              "aggs": {
                "extended_stats_age": {
                  "extended_stats": {
                    "field": "age"
                  }
                }
              }
            }
     */
    private static void extendedStatsAggregation(String indexName, String typeName, String field) {

        MetricsAggregationBuilder aggregation = AggregationBuilders.extendedStats("agg").field(field);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        ExtendedStats agg = sResponse.getAggregations().get("agg");
        double min = agg.getMin();
        double max = agg.getMax();
        double avg = agg.getAvg();
        double sum = agg.getSum();
        long count = agg.getCount();
        double stdDeviation = agg.getStdDeviation();
        double sumOfSquares = agg.getSumOfSquares();
        double variance = agg.getVariance();
        logger.info("min ：" + min);
        logger.info("max ：" + max);
        logger.info("avg ：" + avg);
        logger.info("sum ：" + sum);
        logger.info("count ：" + count);
        logger.info("stdDeviation ：" + stdDeviation);
        logger.info("sumOfSquares ：" + sumOfSquares);
        logger.info("variance ：" + variance);
    }




}
