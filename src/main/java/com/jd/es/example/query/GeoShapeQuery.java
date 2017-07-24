package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;

/**
 * Title: GeoShapeQuery
 * Description: GeoShapeQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/13
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class GeoShapeQuery {

    private static Logger logger = Logger.getLogger(GeoShapeQuery.class);

    public static void main(String[] args) {
        testGeoShapeQuery("example_index", "type1");
    }

    /*  HTTP 接口
        URL:
        TYPE：POST
        BODY:

     */
    private static void testGeoShapeQuery(String indexName, String typeName) {

        QueryBuilder qb = QueryBuilders.geoShapeQuery(
                "shape",
                ShapeBuilder.newMultiPoint()
                        .point(100, 0)
                        .point(101, 0)
                        .point(101, 1)
                        .point(100, 1)
                        .point(100, 0),
                ShapeRelation.INTERSECTS );//relation can be ShapeRelation.WITHIN, ShapeRelation.INTERSECTS or ShapeRelation.DISJOINT

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}
