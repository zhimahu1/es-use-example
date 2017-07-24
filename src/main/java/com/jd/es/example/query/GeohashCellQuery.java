package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: GeohashCellQuery
 * Description: GeohashCellQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/13
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class GeohashCellQuery {

    private static Logger logger = Logger.getLogger(GeohashCellQuery.class);

    public static void main(String[] args) {
        testGeohashCellQuery("example_index", "type1");
    }


    /** 添加field
    "properties": {
        "cell": {
            "type": "geo_point",
                    "geohash": true,
                    "geohash_prefix": true,
                    "geohash_precision": 10
        }
    }
     */

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
                    "geohash_cell": {
                      "cell": {
                        "lat": 50.408,
                        "lon": 50.5186
                      },
                      "precision": 3,
                      "neighbors": true
                    }
                  }
                }
              }
            }
     */
    private static void testGeohashCellQuery(String indexName, String typeName) {

        QueryBuilder qb = QueryBuilders.geoHashCellQuery("cell",
                new GeoPoint(50, 52))
                .neighbors(true)
                .precision(3);

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}
