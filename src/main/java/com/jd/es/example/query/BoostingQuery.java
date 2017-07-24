package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: BoostingQuery
 * Description: BoostingQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class BoostingQuery {

    private static Logger logger = Logger.getLogger(BoostingQuery.class);

    public static void main(String[] args) {
        testBoostingQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "country", "China","gender","male");

        testBoostingQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "country", "China","country","American");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "boosting": {
                  "positive": {
                    "term": {
                      "country": "China"
                    }
                  },
                  "negative": {
                    "term": {
                      "gender": "male"
                    }
                  },
                  "negative_boost": 0.2
                }
              }
            }
     */
    private static void testBoostingQuery(String indexName, String typeName, String pName, String pValue, String nName, String nValue) {

        QueryBuilder qb = QueryBuilders.boostingQuery()
                .positive(QueryBuilders.termQuery(pName, pValue))//使文档打分增加
                .negative(QueryBuilders.termQuery(nName, nValue))//使文档打分减少
                .negativeBoost(0.2f);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}
