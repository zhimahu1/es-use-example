package com.jd.es.example.aggregations;


import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsBuilder;

/**
 * Title: GeoBoundsAggregation  TODO 搞个列子，加地址field
 * Description: GeoBoundsAggregation
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/8
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class GeoBoundsAggregation {
    private static Logger logger = Logger.getLogger(com.jd.es.example.aggregations.GeoBoundsAggregation.class);

    public static void main(String[] args) {
        geoBoundsAggregation(Tool.INDEX_NAME, Tool.TYPE_NAME, "address.location");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:

     */
    private static void geoBoundsAggregation(String indexName, String typeName, String field) {

        GeoBoundsBuilder aggregation = AggregationBuilders.geoBounds("agg").field(field).wrapLongitude(true);

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).addAggregation(aggregation).execute().actionGet();

        GeoBounds agg = sResponse.getAggregations().get("agg");
        GeoPoint bottomRight = agg.bottomRight();
        GeoPoint topLeft = agg.topLeft();
        logger.info("bottomRigh:" + bottomRight + " topLeft:" + topLeft);
    }
}
