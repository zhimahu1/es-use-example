package com.jd.es.example.query;

import com.jd.es.example.common.Tool;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Title: ConstantScoreQuery https://www.elastic.co/guide/en/elasticsearch/reference/2.1/query-dsl-constant-score-query.html
 * Description: ConstantScoreQuery
 * Company: <a href=www.jd.com>京东</a>
 * Date:  2016/7/12
 *
 * @author <a href=mailto:zhouzhichao@jd.com>chaochao</a>
 */
public class ConstantScoreQuery {

    private static Logger logger = Logger.getLogger(ConstantScoreQuery.class);

    public static void main(String[] args) {
        testConstantScoreQuery(Tool.INDEX_NAME, Tool.TYPE_NAME, "name", "c1");
    }

    /*  HTTP 接口
        URL:agg_index/agg_type/_search
        TYPE：POST
        BODY:
            {
              "query": {
                "constant_score": {
                  "filter": {
                    "term": {
                      "name": "c1"
                    }
                  },
                  "boost": 1.2
                }
              }
            }
     */
    public static void testConstantScoreQuery(String indexName, String typeName, String fieldName, String fieldValue) {

        QueryBuilder qb = QueryBuilders.constantScoreQuery(
                QueryBuilders.termQuery(fieldName, fieldValue)
        ).boost(2.0f);

        logger.info(qb);
        SearchResponse sResponse = Tool.CLIENT.prepareSearch(indexName).setTypes(typeName).setQuery(qb).get();
        logger.info(sResponse);
    }

/**
 * 如果写多个filter，只有最后一个filter生效。不知道是不是es对于固定分值查询不支持多个filter。
 * 如下语句只会过滤"country": "China"。
    {
        "query": {
        "constant_score": {
            "filter": [
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
            ],
            "boost": 1.2
        }
    }
    }

 */

    // https://www.elastic.co/guide/en/elasticsearch/guide/2.x/_dealing_with_null_values.html#_exists_query
}