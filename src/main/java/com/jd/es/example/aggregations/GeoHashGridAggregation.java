package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;

/**
 * Title: GeoHashGridAggregation
 * Description: GeoHashGridAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/11
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class GeoHashGridAggregation {

    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.GeoHashGridAggregation.class);

    public static void main(String[] args) {
        geoHashGridAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "address.location");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:

     */
    private static void geoHashGridAggregation(String indexName, String typeName, String field) {

        AggregationBuilder aggregation =
                AggregationBuilders
                        .geohashGrid("agg")
                        .field(field)
                        .precision(4);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        GeoHashGrid agg = sResponse.getAggregations().get("agg");

        for (GeoHashGrid.Bucket entry : agg.getBuckets()) {
            String keyAsString = entry.getKeyAsString(); // key as String
            GeoPoint key = (GeoPoint) entry.getKey();    // key as geo point
            long docCount = entry.getDocCount();         // Doc count
            logger.info("key:" + keyAsString + " point:" + key + " doc_count:" + docCount);
        }
    }
}
