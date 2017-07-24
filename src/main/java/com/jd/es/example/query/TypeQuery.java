package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: TypeQuery
 * Description: TypeQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class TypeQuery {

    private static Logger logger = Logger.getLogger(TypeQuery.class);

    public static void main(String[] args) {
        testTypeQuery(Tool.INDEX_NAME, "agg_type");
    }

    /*  HTTP 接口
        URL:agg_index/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "type": {
                  "value": "agg_type"
                }
              }
            }
     */
    private static void testTypeQuery(String indexName, String typeName) {

        QueryBuilder qb = QueryBuilders.typeQuery(typeName);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setQuery(qb).get();
        logger.info(sResponse);
    }
}
