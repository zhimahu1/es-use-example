package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: FuzzyQuery
 * Description: FuzzyQuery fuzzy对于你查询的内容不进行分词处理，与term一样。
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class FuzzyQuery {

    private static Logger logger = Logger.getLogger(FuzzyQuery.class);

    public static void main(String[] args) {
        testFuzzyQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "gender", "male");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "fuzzy": {
                  "gender": "male"
                }
              }
            }

        BODY2:
            {
              "query": {
                "fuzzy": {
                  "gender": {
                    "value": "male",
                    "fuzziness":2
                  }
                }
              }
            }
     */
    private static void testFuzzyQuery(String indexName, String typeName, String fieldName, String fuzzy) {
        QueryBuilder qb = QueryBuilders.fuzzyQuery(
                fieldName,
                fuzzy
        );
        //.fuzziness(Fuzziness.build(2));//模糊两位，可以匹配到female

        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }
}
