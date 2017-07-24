package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: IndicesQuery
 * Description: IndicesQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class IndicesQuery {

    private static Logger logger = Logger.getLogger(IndicesQuery.class);

    public static void main(String[] args) {
        testIndicesQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "gender", "male");
    }

    /*  HTTP 接口
        URL:_search
        TYPE：POST
        BODY:
            {
              "query": {
                "indices": {
                  "indices": [
                    "agg_index",
                    "agg_index2"
                  ],
                  "query": {
                    "term": {
                      "gender": "male"
                    }
                  },
                  "no_match_query": {
                    "term": {
                      "gender": "male"
                    }
                  }
                }
              }
            }
     */
    private static void testIndicesQuery(String indexName, String typeName, String fieldName, String fieldValue) {

        QueryBuilder qb = QueryBuilders.indicesQuery(
                QueryBuilders.termQuery(fieldName, fieldValue),
                "agg_index", "agg_index2"
        ).noMatchQuery(QueryBuilders.termQuery("gender", "male"));// Using another query when no match for the main one
        //.noMatchQuery("all");//不在上面指定的index list中的索引，使用这个。 none (to match no documents), and all (to match all documents). Defaults to all.

        //SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        SearchResponse sResponse = Tool.CLIENT.prepareSearch().setQuery(qb).get();//不需要指定索引，类型，直接在整个集群上查询
        logger.info(sResponse);
    }
}
