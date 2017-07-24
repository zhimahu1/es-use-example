package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.Range;

/**
 * Title: IpRangeAggregation
 * Description: IpRangeAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/11
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class IpRangeAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.IpRangeAggregation.class);

    public static void main(String[] args) {
        ipRangeAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "ip");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:

     */
    private static void ipRangeAggregation(String indexName, String typeName, String field) {

        AggregationBuilder aggregation =
                AggregationBuilders
                        .ipRange("agg")
                        .field(field)
                        .addUnboundedTo("192.168.1.0")             // from -infinity to 192.168.1.0 (excluded)
                        .addRange("192.168.1.0", "192.168.2.0")    // from 192.168.1.0 to 192.168.2.0 (excluded)
                        .addUnboundedFrom("192.168.2.0");          // from 192.168.2.0 to +infinity

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        Range agg = sResponse.getAggregations().get("agg");

        for (Range.Bucket entry : agg.getBuckets()) {
            String key = entry.getKeyAsString();            // Ip range as key
            String fromAsString = entry.getFromAsString();  // Ip bucket from as a String
            String toAsString = entry.getToAsString();      // Ip bucket to as a String
            long docCount = entry.getDocCount();            // Doc count
            logger.info("key:" + key + " from:" + fromAsString + " to:" + toAsString + " doc_count:" + docCount);
        }
    }
}
