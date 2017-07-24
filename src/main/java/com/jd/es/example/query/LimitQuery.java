package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: LimitQuery 不推荐
 * Description: LimitQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
@Deprecated
public class LimitQuery {

    private static Logger logger = Logger.getLogger(LimitQuery.class);

    public static void main(String[] args) {
        testLimitQuery(Tool.INDEX_NAME, Tool.TYPE_NAME,  2);//TODO 没效果啊啊啊啊
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "filtered": {
                  "filter": {
                    "limit": {
                      "value": 2
                    }
                  },
                  "query": {
                    "term": {
                      "country": "China"
                    }
                  }
                }
              }
            }
    */
    private static void testLimitQuery(String indexName, String typeName,int limitSize) {

        QueryBuilder qb = QueryBuilders.limitQuery(limitSize);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}
