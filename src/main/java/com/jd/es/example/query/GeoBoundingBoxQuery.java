package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: GeoBoundingBoxQuery
 * Description: GeoBoundingBoxQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/13
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class GeoBoundingBoxQuery {

    private static Logger logger = Logger.getLogger(GeoBoundingBoxQuery.class);

    public static void main(String[] args) {
        testGeoBoundingBoxQuery("example_index", "type1");
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
                    "geo_bounding_box": {
                      "address": {
                        "top_left": {
                          "lat": 40.0,
                          "lon": -81.34
                        },
                        "bottom_right": {
                          "lat": -80.01,
                          "lon": 91.12
                        }
                      }
                    }
                  }
                }
              }
            }
     */
    private static void testGeoBoundingBoxQuery(String indexName, String typeName) {

        QueryBuilder qb = QueryBuilders.geoBoundingBoxQuery("address")
                .topLeft(40.73, -74.1)
                .bottomRight(-40.717, 90.99);

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}
