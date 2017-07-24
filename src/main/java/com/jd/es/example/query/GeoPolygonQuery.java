package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: GeoPolygonQuery
 * Description: GeoPolygonQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/13
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class GeoPolygonQuery {

    private static Logger logger = Logger.getLogger(GeoPolygonQuery.class);

    public static void main(String[] args) {
        testGeoPolygonQuery("example_index", "type1");
    }

    /*  HTTP 接口
        URL:example_index/type1/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "bool": {
                  "filter": {
                    "geo_polygon": {
                      "address": {
                        "points": [
                          {
                            "lat": 80,
                            "lon": -75
                          },
                          {
                            "lat": 80,
                            "lon": -45
                          },
                          {
                            "lat": 20,
                            "lon": -45
                          },
                          {
                            "lat": 20,
                            "lon": -75
                          }
                        ]
                      }
                    }
                  }
                }
              }
            }
     */
    private static void testGeoPolygonQuery(String indexName, String typeName) {

        QueryBuilder qb = QueryBuilders.geoPolygonQuery("address")
                .addPoint(80, -75)
                .addPoint(80, -45)
                .addPoint(20, -45)
                .addPoint(20, -75);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}
