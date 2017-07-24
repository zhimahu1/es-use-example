package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;

/**
 * Title: HistogramAggregation
 * Description: HistogramAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/11
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class HistogramAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.HistogramAggregation.class);

    public static void main(String[] args) {
        histogramAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "height");
        histogramAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "age");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:

     */
    private static void histogramAggregation(String indexName, String typeName, String field) {

        AggregationBuilder aggregation =
                AggregationBuilders
                        .histogram("agg")
                        .field(field)
                        .interval(1);//.minDocCount(1);

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        Histogram agg = sResponse.getAggregations().get("agg");

        for (Histogram.Bucket entry : agg.getBuckets()) {
            Long key = (Long) entry.getKey();       // Key
            long docCount = entry.getDocCount();    // Doc count
            logger.info("key:" + key + ", doc_count " + docCount);
        }
    }
}
