package com.jd.es.example.aggregations;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.joda.time.DateTime;

/**
 * Title: DateHistogramAggregation
 * Description: DateHistogramAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/11
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class DateHistogramAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.DateHistogramAggregation.class);

    public static void main(String[] args) {
        dateHistogramAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "dateOfBirth");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:

     */
    private static void dateHistogramAggregation(String indexName, String typeName, String field) {

        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateHistogram("agg")
                        .field(field)
                        .interval(DateHistogramInterval.YEAR);//.minDocCount(1);
        //.interval(DateHistogramInterval.days(10));//以10天为间隔:

        SearchRequestBuilder srb = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation);
        logger.info(srb);
        SearchResponse sResponse = srb.get();
        logger.info(sResponse);
        Histogram agg = sResponse.getAggregations().get("agg");

        for (Histogram.Bucket entry : agg.getBuckets()) {
            DateTime key = (DateTime) entry.getKey();    // Key
            String keyAsString = entry.getKeyAsString(); // Key as String
            long docCount = entry.getDocCount();         // Doc count
            logger.info("key:" + keyAsString + " date:" + key.getYear() + ", doc_count " + docCount);
        }
    }
}
