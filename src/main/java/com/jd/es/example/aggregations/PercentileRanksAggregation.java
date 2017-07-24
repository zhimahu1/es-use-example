package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks;

/**
 * Title: PercentileRanksAggregation
 * Description: PercentileRanksAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class PercentileRanksAggregation {
    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.PercentileRanksAggregation.class);

    public static void main(String[] args) {
        //percentileRanksAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "height");
        percentileRanksAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "age");
    }


    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "size":0,
              "aggregations": {
                "agg": {
                  "percentile_ranks": {
                    "field": "age",
                    "values": [
                      0,
                      1,
                      2,
                      7,
                      8,
                      8.1,
                      50,
                      100
                    ]
                  }
                }
              }
            }
     */
    private static void percentileRanksAggregation(String indexName, String typeName, String field) {

        MetricsAggregationBuilder aggregation = AggregationBuilders
                .percentileRanks("agg")
                .field(field)
                .percentiles(0.0,1.0, 2.0, 7.0,8.0,8.1,50.0,100.0);
        SearchRequestBuilder searchRequestBuilder = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation);
        logger.info(searchRequestBuilder);

        SearchResponse sResponse = searchRequestBuilder.get();
        logger.info(sResponse);

        PercentileRanks agg = sResponse.getAggregations().get("agg");
        // For each entry
        for (Percentile entry : agg) {
            double percent = entry.getPercent();    // Percent
            double value = entry.getValue();        // Value

            logger.info("percent:" + percent + " value:" + value);
        }
    }
}
