package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: AndQuery
 * Description: AndQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
@Deprecated
public class AndQuery {

    private static Logger logger = Logger.getLogger(AndQuery.class);

    public static void main(String[] args) {
        testAndQuery(Tool.INDEX_NAME, Tool.TYPE_NAME);
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "and": [
                  {
                    "term": {
                      "gender": "female"
                    }
                  },
                  {
                    "term": {
                      "country": "China"
                    }
                  }
                ]
              }
            }
     */
    private static void testAndQuery(String indexName, String typeName) {

        QueryBuilder qb = QueryBuilders.andQuery(
                QueryBuilders.termQuery("gender", "female"),
                QueryBuilders.termQuery("country", "China"));

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}
