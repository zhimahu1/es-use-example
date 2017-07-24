package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: PrefixQuery
 * Description: PrefixQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class PrefixQuery {

    private static Logger logger = Logger.getLogger(PrefixQuery.class);

    public static void main(String[] args) {
        testPrefixQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "name", "c");
        testPrefixQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "name", "a");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "prefix": {
                  "name": "c"
                }
              }
            }
     */
    private static void testPrefixQuery(String indexName, String typeName, String fieldName, String prefix) {
        QueryBuilder qb = QueryBuilders.prefixQuery(
                fieldName,
                prefix
        );
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}