package com.jd.es.example.aggregations;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.joda.time.DateTime;

/**
 * Title: DateRangeAggregation
 * Description: DateRangeAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/11
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class DateRangeAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.DateRangeAggregation.class);

    public static void main(String[] args) {
        dateRangeAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "dateOfBirth");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:

     */
    private static void dateRangeAggregation(String indexName, String typeName, String field) {

        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateRange("agg")
                        .field(field)
                        .format("yyyy")
                        .addUnboundedTo("2011")    // from -infinity to 2011 (excluded)
                        .addRange("2011", "2012")  // from 2011 to 2012 (excluded)
                        .addUnboundedFrom("2012"); // from 2012 to +infinity
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        Range agg = sResponse.getAggregations().get("agg");

        for (Range.Bucket entry : agg.getBuckets()) {
            String key = entry.getKeyAsString();                // Date range as key
            DateTime fromAsDate = (DateTime) entry.getFrom();   // Date bucket from as a Date
            DateTime toAsDate = (DateTime) entry.getTo();       // Date bucket to as a Date
            long docCount = entry.getDocCount();                // Doc count

            logger.info("key:"+key+" from:"+fromAsDate+" to:"+toAsDate+" doc_count:" +docCount);
        }
    }
}
