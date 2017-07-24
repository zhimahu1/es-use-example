package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: NotQuery
 * Description: NotQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
@Deprecated
public class NotQuery {

    private static Logger logger = Logger.getLogger(NotQuery.class);

    public static void main(String[] args) {
        testNotQuery(Tool.INDEX_NAME, Tool.TYPE_NAME);
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "not": {
                  "range": {
                    "age": {
                      "from": "1",
                      "to": "5"
                    }
                  }
                }
              }
            }
     */
    private static void testNotQuery(String indexName, String typeName) {

        QueryBuilder qb = QueryBuilders.notQuery(
                QueryBuilders.rangeQuery("age").from("1").to("2")//返回不匹配的
        );
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}
