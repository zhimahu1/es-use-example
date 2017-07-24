package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: OrQuery
 * Description: OrQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
@Deprecated
public class OrQuery {

    private static Logger logger = Logger.getLogger(OrQuery.class);

    public static void main(String[] args) {
        testOrQuery(Tool.INDEX_NAME, Tool.TYPE_NAME);
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "or": [
                  {
                    "term": {
                      "name": "c1"
                    }
                  },
                  {
                    "term": {
                      "country": "American"
                    }
                  }
                ]
              }
            }
     */
    private static void testOrQuery(String indexName, String typeName) {

        QueryBuilder qb = QueryBuilders.orQuery(
                QueryBuilders.rangeQuery("age").from(1).to(2),
                QueryBuilders.matchQuery("country", "American")
        );
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}
