package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: GeoDistanceRangeQuery
 * Description: GeoDistanceRangeQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/13
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class GeoDistanceRangeQuery {

    private static Logger logger = Logger.getLogger(GeoDistanceRangeQuery.class);

    public static void main(String[] args) {
        testGeoDistanceRangeQuery("example_index", "type1");
    }

    /*  HTTP 接口
        URL:example_index/type1/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "bool": {
                  "must": {
                    "match_all": {}
                  },
                  "filter": {
                    "geo_distance_range": {
                      "from": "100km",
                      "to": "200km",
                      "address": {
                        "lat": 40,
                        "lon": -70
                      }
                    }
                  }
                }
              }
            }
     */
    private static void testGeoDistanceRangeQuery(String indexName, String typeName) {

        QueryBuilder qb = QueryBuilders.geoDistanceRangeQuery("address")
                .point(40, -70)
                .from("100km")//.from("180km")// starting distance from center point
                .to("200km")//ending distance from center point
                .includeLower(true)//include lower value means that from is gt when false or gte when true
                .includeUpper(false)//include upper value means that to is lt when false or lte when true
                .optimizeBbox("memory")//optimize bounding box: memory, indexed or none
                .geoDistance(GeoDistance.ARC);//distance computation mode: GeoDistance.SLOPPY_ARC (default), GeoDistance.ARC (slightly more precise but significantly slower) or GeoDistance.PLANE (faster, but inaccurate on long distances and close to the poles)

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}
