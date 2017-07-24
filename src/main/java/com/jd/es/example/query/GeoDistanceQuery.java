package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: GeoDistanceQuery
 * Description: GeoDistanceQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/13
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class GeoDistanceQuery {

    private static Logger logger = Logger.getLogger(GeoDistanceQuery.class);

    public static void main(String[] args) {
        testGeoDistanceQuery("example_index", "type1");
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
                    "geo_distance": {
                      "distance": "200km",
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
    private static void testGeoDistanceQuery(String indexName, String typeName) {

        QueryBuilder qb = QueryBuilders.geoDistanceQuery("address")
                .point(40, -70)
                .distance(200, DistanceUnit.KILOMETERS)
                .optimizeBbox("memory")
                .geoDistance(GeoDistance.ARC);//distance computation mode: GeoDistance.SLOPPY_ARC (default), GeoDistance.ARC (slightly more precise but significantly slower) or GeoDistance.PLANE (faster, but inaccurate on long distances and close to the poles)

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}
