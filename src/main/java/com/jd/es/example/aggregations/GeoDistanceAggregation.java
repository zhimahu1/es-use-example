package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.Range;

/**
 * Title: GeoDistanceAggregation
 * Description: GeoDistanceAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/11
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class GeoDistanceAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.GeoDistanceAggregation.class);

    public static void main(String[] args) {
        geoDistanceAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "address.location");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:

     */
    private static void geoDistanceAggregation(String indexName, String typeName, String field) {

        AggregationBuilder aggregation =
                AggregationBuilders
                        .geoDistance("agg")
                        .field(field)
                        .point(new GeoPoint(48.84237171118314, 2.33320027692004))
                        .unit(DistanceUnit.KILOMETERS)
                        .addUnboundedTo(3.0)
                        .addRange(3.0, 10.0)
                        .addRange(10.0, 500.0);

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        Range agg = sResponse.getAggregations().get("agg");
        for (Range.Bucket entry : agg.getBuckets()) {
            String key = entry.getKeyAsString();    // key as String
            Number from = (Number) entry.getFrom(); // bucket from value
            Number to = (Number) entry.getTo();     // bucket to value
            long docCount = entry.getDocCount();    // Doc count
            logger.info("key:" + key + " from:" + from + " to:" + to + " doc_count:" + docCount);
        }
    }
}
