package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: IdsQuery
 * Description: IdsQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class IdsQuery {

    private static Logger logger = Logger.getLogger(IdsQuery.class);

    public static void main(String[] args) {
        //testIdsQuery(Tool.INDEX_NAME, Tool.TYPE_NAME);
        testIdsQuery("agg_index_alias", Tool.TYPE_NAME);//使用alias
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "ids": {
                  "type": "agg_type",
                  "values": [
                    "1",
                    "4"
                  ]
                }
              }
            }
     */
    private static void testIdsQuery(String indexName, String typeName) {

        //QueryBuilder qb = QueryBuilders.idsQuery("my_type", "type2").addIds("1", "3", "5");
        QueryBuilder qb = QueryBuilders.idsQuery().addIds("1", "3", "5");

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}
