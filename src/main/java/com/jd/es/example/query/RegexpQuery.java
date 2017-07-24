package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: RegexpQuery
 * Description: RegexpQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class RegexpQuery {

    private static Logger logger = Logger.getLogger(RegexpQuery.class);

    public static void main(String[] args) {
        testRegexpQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "gender", "(fe)?.*");
        testRegexpQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "gender", "m.*");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "regexp": {
                  "gender": "(fe)?.*"
                }
              }
            }
     */
    private static void testRegexpQuery(String indexName, String typeName, String fieldName, String regexp) {
        QueryBuilder qb = QueryBuilders.regexpQuery(
                fieldName,
                regexp);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}